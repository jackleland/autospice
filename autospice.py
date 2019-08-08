import configparser
from pathlib import Path
from collections import OrderedDict
from pprint import pprint
from datetime import timedelta
import subprocess
import machine as mch
import codes
import os
import shutil
import math
from logger import Logger

from humanfriendly import format_timespan
import click

SUPPORTED_MACHINES = {
    'marconi': mch.marconi_skl,
    'marconi_long': mch.marconi_skl_fuaspecial,
    'cumulus': mch.cumulus
}
# TODO: Auto-populate this with ast
# TODO: Make default code with generic options?
SUPPORTED_CODES = {
    'spice': codes.Spice()
}


@click.command()
@click.argument('config_file', type=click.Path(exists=True))
@click.option('--dryrun_fl', '-d', default=False, is_flag=True)
def submit_job(config_file, dryrun_fl=False):
    """
    Reads a YAML-like configuration file, writes a job script, and submits a
    simulation job based on the options contained in the file.

    The config file is just a text file which conforms to YAML-style syntax,
    and has the structure shown in the example below:

    [scheduler]
      name:       Simulation
      machine:    marconi
      user:       tnichola
      walltime:   8:00:00
      queue:      skl_fua_prod
      account:    FUSIO_ru3CCFE
      n_cpus:     48

    [code]
      input:      ./BOUT.inp
      output:     ./out/
      executable: ./folder/storm.exe

    [bout]
      restart:    False
      append:     False

    No defaults are provided for any parameters in the config file so that the
    file must contain a complete record of all the job options used for the
    simulation.

    Parameters
    ----------
    config_file : str or path-like
        Path to the configuration file. Specified as a command-line argument
        through click, e.g. $ python3 autostorm.py ./path/to/config.yml

    dryrun_fl : bool
        Boolean flag denoting whether a dry run is being performed or not. If
        performing a dry run no file i/o or submissions will take place.
    """

    # Read and parse the config file
    # TODO: Getting the autospice dir should be more rigorous
    autospice_dir = Path.cwd()
    config_file = autospice_dir / Path(config_file)
    config = configparser.ConfigParser()
    config.read(config_file)

    scheduler_opts = config['scheduler']
    code_opts = config['code']

    # Check Initial, Universal Scheduler Options
    machine_name = scheduler_opts['machine']
    user = scheduler_opts['user']
    if machine_name.lower() not in SUPPORTED_MACHINES:
        raise NotImplementedError("This script assumes you're submitting a job on one of {SUPPORTED_MACHINES}")
    print(f"User {user} on machine {machine_name} \n")

    # Check code is supported
    code_name = code_opts['code_name']
    if code_name not in SUPPORTED_CODES:
        raise NotImplementedError("This script only supports the use of certain codes."
                                  f"Currently implemented codes are: {list(SUPPORTED_CODES.keys())}")

    # Create SimulationCode object and Machine objects
    sim_code = SUPPORTED_CODES[code_name]
    machine = SUPPORTED_MACHINES[machine_name]

    cpus_per_node, cpus_tot, email, job_name, memory_req, n_jobs, nodes, walltime = process_scheduler_opts(
        machine, scheduler_opts)

    # ---------------- Check Code Options ----------------

    executable_dir = Path(code_opts['bin'])
    input_file = Path(code_opts['input'])
    output_dir = Path(code_opts['output'])

    # Change directory to bin location
    print(f"Changing directory to {executable_dir} \n")
    if executable_dir.resolve().is_dir():
        os.chdir(executable_dir)
    else:
        raise ValueError('The "bin" variable must be a valid directory with a binary in it.\n'
                         f'{executable_dir}')

    # Check input file exists
    if not input_file.is_file():
        raise FileNotFoundError(f"No input file found at {input_file}")

    sim_code.verify_input_file(input_file)
    param_scan_fl = sim_code.is_parameter_scan(input_file)

    # Check executable file exists
    executable = Path(code_opts['executable'])
    if not executable.is_file() and not dryrun_fl:
        raise FileNotFoundError(f"No executable file found at {executable}")

    if param_scan_fl:
        scan_param, inp_parser = sim_code.get_scanning_parameters(input_file)
        scan_param = scan_param[0]
        print(f"Submitting a parameter scan, scanning over \'{scan_param['parameter']}\' with the following values: \n")
        for value in scan_param['values']:
            print(f'\t{value}')
        print('\n')
    else:
        scan_param = {'values': [None]}
        inp_parser = None

    code_specific_opts = sim_code.process_config_options(config[code_name])
    output_dir = sim_code.directory_io(output_dir, code_specific_opts, dryrun_fl)
    if not dryrun_fl:
        shutil.copy(input_file, output_dir)
        shutil.copy(config_file, output_dir)

    print_choices(scheduler_opts, code_opts, executable_dir / input_file, executable_dir / output_dir,
                  executable_dir / executable, cpus_tot, nodes)
    sim_code.print_config_options(code_specific_opts)

    # TODO: This has been temporarily removed as the syntax has changed from Tom's example script.
    # git_check(executable)

    input_file_base = input_file
    output_dir_base = output_dir

    if click.confirm('\nDo you want to continue?', default=True):
        for param_value in scan_param['values']:
            if param_value is not None:
                param_dir = f"{scan_param['parameter']}_{param_value}"
                output_dir = output_dir_base / param_dir
                if not dryrun_fl:
                    os.makedirs(output_dir, exist_ok=True)

                input_file = output_dir / 'input.inp'
                inp_parser[scan_param['section']][scan_param['parameter']] = param_value
                if not dryrun_fl:
                    with open(input_file, 'w') as f:
                        inp_parser.write(f)

            submission_params = {
                'job_name': job_name,
                'nodes': nodes,
                'cpus_per_node': cpus_per_node,
                'walltime': walltime,
                'out_log': output_dir / f'{codes.LOG_PREFIX}.out',
                'err_log': output_dir / f'{codes.LOG_PREFIX}.err',
                'queue': scheduler_opts['queue'],
                'memory': memory_req,
                'account': scheduler_opts['account'],
                'email': email,
                'email_events': machine.scheduler.default_email_settings
            }
            call_params = {
                'cpus_tot': cpus_tot,
                'executable': executable,
                'executable_dir': executable_dir,
                'output_dir': output_dir,
                'input_file': input_file,
                'config_opts': code_specific_opts
            }

            job_script = write_job_script(submission_params, machine, sim_code, call_params, label='_0',
                                          dryrun_fl=dryrun_fl)

            # Submit job script
            if dryrun_fl:
                print(f"Job script written as: \n"
                      f"{job_script}\n")
            else:
                out = subprocess.check_output(['sbatch', str(job_script)])
                *rest, job_num = str(out, 'utf-8').split(' ')
                job_num = job_num.strip()
                print(f"\nSubmitted job number {job_num}")

                jobs = [job_num, ]
                if n_jobs > 1:
                    job_script = write_job_script(submission_params, machine, sim_code, call_params, label='_1',
                                                  multi_submission=True)

                    for i in range(n_jobs - 1):
                        out = subprocess.check_output(['sbatch', '-d', f'afterany:{job_num}', str(job_script)])
                        *rest, job_num = str(out, 'utf-8').split(' ')
                        job_num = job_num.strip()
                        print(f"\nSubmitted multisubmission {i}, job number {job_num}")
                        jobs.append(job_num)

                with open(output_dir / 'jobs.txt', 'w') as f:
                    for job in jobs:
                        f.write(f'{job}\n')

                # Log the submission to a google sheet using logger
                # TODO: (2019-07-15) Expand to include n_jobs and param_scan_fl
                # TODO: (2019-07-17) api_json_filename should be specified by a config file option, as should whether
                #  the logger runs
                logger = Logger(api_json_filename=str(autospice_dir / 'client_secret.json'))
                logger.update_log({
                    'machine': machine_name,
                    'job_number': jobs[0],
                    'job_name': job_name,
                    'input_file': str(input_file),
                    'masala_config': str(config_file),
                    'nodes': nodes,
                    'total_cores': cpus_tot,
                    'memory_req': memory_req,
                    'wtime_req': walltime,
                    'notes': ''
                })
    else:
        if not dryrun_fl:
            shutil.rmtree(output_dir)


def process_scheduler_opts(machine, scheduler_opts):
    # TODO: This should be moved into Scheduler, with a specific section for implementation specific stuff and a
    #       standard format outputted
    job_name = scheduler_opts['job_name']
    email = scheduler_opts['email']
    print(f"Job completion and error notifications will be sent to {email} \n")

    # Check if number of processors is sensible for this machine
    n_cpus = scheduler_opts['n_cpus']
    try:
        n_cpus = int(n_cpus)
    except:
        raise TypeError("Can't use a non-integer number of CPUs")

    if 'nodes' not in scheduler_opts:
        nodes, cpus_per_node = machine.calc_nodes(n_cpus)
    else:
        try:
            nodes = int(scheduler_opts['nodes'])
        except:
            raise TypeError("Can't use a non-integer number of CPUs")
        cpus_per_node = machine.check_nodes(n_cpus, nodes)

    memory_req = int(scheduler_opts['memory'])
    if memory_req > machine.memory_per_node * nodes:
        # TODO: Memory should be able to be prioritised above maximising cpus_per_node
        print(f"WARNING: Requested amount of memory exceeds the maximum available on {machine.name}. With {nodes} \n"
              f"node(s) the maximum amount of available memory is {machine.memory_per_node * nodes}GB, the job will \n"
              f"be submitted with this amount requested. To submit the job with {memory_req}GB of memory, you would \n"
              f"require {int(math.ceil(memory_req / machine.memory_per_node))} nodes.\n")
        memory_req = machine.memory_per_node * nodes

    # TODO: verify string is in correct format
    walltime = scheduler_opts['walltime']
    n_jobs = machine.get_n_jobs(walltime, safe_job_time_fl=True)
    if n_jobs > 1:
        print(f"Walltime requested ({walltime}) exceeds the maximum available walltime for a single job on \n"
              f"{machine.name}. The job will be split into {n_jobs} to complete successfully.")
        walltime = f"{machine.max_job_time}:00:00"

    return cpus_per_node, n_cpus, email, job_name, memory_req, n_jobs, nodes, walltime


def print_choices(scheduler_opts, code_opts, full_input, full_output, full_exe_path, cpus, nodes):
    print("\nChosen options for simulation run are:")
    options = OrderedDict([
        ('Run name', scheduler_opts['job_name']),
        ('Machine', scheduler_opts['machine']),
        ('Code', code_opts['code_name']),
        ('Input file path', str(full_input)),
        ('Output directory', str(full_output)),
        ('Executable file path', str(full_exe_path)),
        ('Queue', scheduler_opts['queue']),
        ('Account', scheduler_opts['account']),
        ('Walltime', scheduler_opts['walltime'])
    ])
    pprint(options)
    print(f"\nWill use {str(cpus)} cpus across {str(nodes)} nodes")

    hrs, min, sec = (int(quantity) for quantity in scheduler_opts['walltime'].split(':'))
    total_walltime = timedelta(hours=hrs, minutes=min, seconds=sec) * cpus
    print(f"Total CPU time requested is {format_timespan(total_walltime)}\n")


def write_job_script(submission_params, machine, code, call_params, multi_submission=False, label='', dryrun_fl=False):
    header = machine.scheduler.get_submission_script_header(submission_params)

    body = code.get_submission_script_body(machine, call_params, multi_submission=multi_submission)

    if not dryrun_fl:
        job_script = Path(str(call_params['output_dir'] / f'melange{label}') + machine.scheduler.script_ext)
        job_script.touch()
        job_script.write_text(header + body)
    else:
        job_script = header + body

    return job_script


if __name__ == '__main__':
    submit_job()
