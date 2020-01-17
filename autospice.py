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
@click.option('--safe_job_time_fl', '-s', default=True, is_flag=True)
@click.option('--backup_fl', '-b', default=True, is_flag=True)
@click.option('--restart_copy_mode', '-r', default='1', type=click.Choice(['0', '1', '2', '3', 'none', 'new', 'stay_in',
                                                                           'stay_out']))
def submit_job(config_file, dryrun_fl=False, safe_job_time_fl=True, backup_fl=True, restart_copy_mode='1'):
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

    safe_job_time_fl : bool
        Boolean flag denoting whether a safe job time should be requested,
        whereby only 90% of the maximum allowed job time on a machine is
        requested to allow the remaining 10% to be used for I/O etc. This
        hopefully stops the maximum allowed job time from interfering with the
        simulation.

    backup_fl : bool
        Boolean flag denoting whether simulation directory should be
        periodically backed up. This will usually be useful at the end of a
        simulation on a machine with a limited job time e.g. Marconi

    restart_copy_mode: int / str
        Option to select the type of copying that happens on restart as a means
        of making a backup of the simulation directory being restarted. Options
        are:
         - 0 or 'none':     No copying done upon restart
         - 1 or 'new':      Directory contents copied to a new directory named
                            [directory]_restart (or, if this already exists,
                            iterations thereof with appended ascending
                            integers). The simulation then runs in this newly
                            created directory. [DEFAULT]
         - 2 or 'stay_out': Directory contents copied to a new directory named
                            [directory]_at_restart (or, if this already exists,
                            iterations thereof with appended ascending
                            integers). The simulation then runs in the original
                            directory.
         - 3 or 'stay_in':  Directory contents copied to a new directory, within
                            the restarting directory, named with the current
                            date and time in the format
                            'backup_at_restart_[YYYYMMDD-HHMM]. The simulation
                            then runs in the original directory.
        The copying in all of these options ignores any folders within the
        original directory starting with 'backup'. Default is 1.

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

    # cpus_per_node, cpus_tot, email, job_name, memory_req, n_jobs, nodes, walltime = process_scheduler_opts(
    #     machine, scheduler_opts)
    submission_params, call_params, n_jobs = process_scheduler_opts(machine, scheduler_opts,
                                                                    safe_job_time_fl=safe_job_time_fl)

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
    output_dir = sim_code.directory_io(output_dir, code_specific_opts, dryrun_fl, restart_copy_mode=restart_copy_mode)
    if not dryrun_fl:
        shutil.copy(input_file, output_dir)
        shutil.copy(config_file, output_dir)

    call_params.update({
        'executable': executable,
        'executable_dir': executable_dir,
        'output_dir': output_dir,
        'input_file': input_file,
        'config_opts': code_specific_opts
    })
    submission_params.update({
        'out_log': output_dir / f'{codes.LOG_PREFIX}.out',
        'err_log': output_dir / f'{codes.LOG_PREFIX}.err',
    })

    print_choices(submission_params, call_params, code_name, machine_name)
    sim_code.print_config_options(code_specific_opts)

    # TODO: This has been temporarily removed as the syntax has changed from Tom's example script.
    # git_check(executable)

    output_dir_base = output_dir
    job_name_base = submission_params['job_name']

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

                submission_params['job_name'] = f'{job_name_base}_{param_value}'
                submission_params['out_log'] = output_dir / f'{codes.LOG_PREFIX}.out'
                submission_params['err_log'] = output_dir / f'{codes.LOG_PREFIX}.err'

                call_params['output_dir'] = output_dir
                call_params['input_file'] = input_file

            job_script = write_job_script(submission_params, machine, sim_code, call_params, label='_0',
                                          dryrun_fl=dryrun_fl, safe_job_time_fl=safe_job_time_fl, backup_fl=backup_fl)

            # Submit job script
            if dryrun_fl:
                print(f"Job script written as: \n"
                      f"{job_script}\n")
            else:
                out = subprocess.check_output([machine.scheduler.submission_command, str(job_script)])
                *rest, job_num = str(out, 'utf-8').split(' ')
                job_num = job_num.strip()
                print(f"\nSubmitted job number {job_num}")

                jobs = [job_num, ]
                if n_jobs > 1:
                    job_script = write_job_script(submission_params, machine, sim_code, call_params, label='_1',
                                                  multi_submission=True, safe_job_time_fl=safe_job_time_fl,
                                                  backup_fl=backup_fl)

                    for i in range(n_jobs - 1):
                        # TODO: (2019-10-10) This is only applicable to slurm, other implementations possible but this
                        # TODO: is only currently necessary because of marconi's time limits.
                        out = subprocess.check_output([machine.scheduler.submission_command, '-d',
                                                       f'afterany:{job_num}', str(job_script)])
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
                    'job_name': submission_params['job_name'],
                    'input_file': str(input_file),
                    'masala_config': str(config_file),
                    'nodes': submission_params['nodes'],
                    'total_cores': call_params['cpus_tot'],
                    'memory_req': submission_params['memory'] if 'memory' in submission_params else 'N/A',
                    'wtime_req': submission_params['walltime'],
                    'notes': ''
                })
    else:
        if not dryrun_fl:
            shutil.rmtree(output_dir)


def process_scheduler_opts(machine, scheduler_opts, safe_job_time_fl=True):
    """
    Function for parsing teh config file and verifying the scheduler options for
    passing to the submission script writer.

    This is split into two parts: required and optional submission parameters
    which are defined within the scheduler. Some of the required parameters need
    to be calculated from information machine-relevant information, whereas some
    can simply be read.

    :param machine:         Machine object created in main autospice script
    :param scheduler_opts:  (section/dict) The section titled 'scheduler' from
                            the configparser containing necessary information
                            for an equivalent dictionary.
    :param safe_job_time_fl:    (boolean) Controls whether a 'safe' time is used
                                for walltime (90% of maximum allowed on machine)
                                to allow time for I/O to occur before job is
                                killed
    :return:                (dict) Submission parameters, in a dictionary
    :return:                (dict) Call parameters, in a dictionary
    :return:                (int) Number of jobs required to be run due to
                            walltime limitations on the machine

    """
    job_name = scheduler_opts['job_name']

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

    # TODO: verify string is in correct format
    walltime = scheduler_opts['walltime']
    n_jobs = machine.get_n_jobs(walltime, safe_job_time_fl=safe_job_time_fl)
    if n_jobs > 1 and not safe_job_time_fl:
        print(f"Walltime requested ({walltime}) exceeds the maximum available walltime for a single job on \n"
              f"{machine.name} - which is {machine.max_job_time}hrs. The job will be split into {n_jobs} to complete \n"
              f"successfully.")
        walltime = f"{machine.max_job_time}:00:00"
    elif n_jobs > 1:
        safe_job_time = machine.get_safe_job_time()
        print(f"Walltime requested ({walltime}) exceeds the maximum available safe walltime for a single job on \n"
              f"{machine.name} - which is {safe_job_time}hrs. The job will be split into {n_jobs} to complete \n"
              f"successfully, but the requested time will remain {machine.max_job_time}:00:00 per job. \n")
        walltime = f"{machine.max_job_time}:00:00"

    optional_submission_params = machine.scheduler.get_optional_submission_params(scheduler_opts)

    # Memory is a special case as it is optional by default but must be verified and possibly recalculated if given.
    if 'memory' in scheduler_opts:
        memory_req = int(scheduler_opts['memory'])
        if memory_req > machine.memory_per_node * nodes:
            # TODO: Memory should be able to be prioritised above maximising cpus_per_node
            print(f"WARNING: Requested amount of memory exceeds the maximum available on {machine.name}. With {nodes}\n"
                  f"node(s) the maximum amount of available memory is {machine.memory_per_node * nodes}GB, the job \n"
                  f"will be submitted with this amount requested. To submit the job with {memory_req}GB of memory, \n"
                  f"you would require {int(math.ceil(memory_req / machine.memory_per_node))} nodes.\n")
            memory_req = machine.memory_per_node * nodes
        optional_submission_params['memory'] = memory_req

    submission_params = {
        'job_name': job_name,
        'nodes': nodes,
        'cpus_per_node': cpus_per_node,
        'walltime': walltime,
        **optional_submission_params
    }

    if 'email' in submission_params and 'email_events' not in submission_params:
        submission_params['email_events'] = machine.scheduler.default_email_settings

    call_params = {
        'cpus_tot': n_cpus,
    }

    ignored_params = set(scheduler_opts.keys()) - set(submission_params.keys()) - {'n_cpus', 'machine', 'user'}
    if len(ignored_params) > 0:
        print(f'WARNING: The following parameters have not been implemented for the scheduler \n'
              f'({machine.scheduler.name}) on {machine.name}: \n'
              f'{ignored_params} \n\n'
              f'These will therefore be ignored on this run.')

    return submission_params, call_params, n_jobs


def print_choices(submission_params, call_params, code_name, machine_name):
    full_input = call_params['executable_dir'] / call_params['input_file']
    full_output = call_params['executable_dir'] / call_params['output_dir']
    full_exe = call_params['executable_dir'] / call_params['executable']

    print("\nChosen options for simulation run are:")
    options = OrderedDict([
        ('Run name', submission_params['job_name']),
        ('Machine', machine_name),
        ('Code', code_name),
        ('Input file path', str(full_input)),
        ('Output directory', str(full_output)),
        ('Executable file path', str(full_exe)),
        ('Walltime', submission_params['walltime'])
    ])
    if 'queue' in submission_params:
        options.update({'Queue': submission_params['queue']})
    if 'account' in submission_params:
        options.update({'Account': submission_params['account']})

    pprint(options)
    print(f"\nWill use {call_params['cpus_tot']} cpus across {submission_params['nodes']} nodes")

    hrs, min, sec = (int(quantity) for quantity in submission_params['walltime'].split(':'))
    total_walltime = timedelta(hours=hrs, minutes=min, seconds=sec) * call_params['cpus_tot']
    print(f"Total CPU time requested is {format_timespan(total_walltime)}\n")


def write_job_script(submission_params, machine, code, call_params, multi_submission=False, label='', dryrun_fl=False,
                     safe_job_time_fl=True, backup_fl=True):
    header = machine.scheduler.get_submission_script_header(submission_params)
    modules = machine.get_submission_script_modules()
    body = code.get_submission_script_body(machine, call_params, multi_submission=multi_submission,
                                           safe_job_time_fl=safe_job_time_fl, backup_fl=backup_fl)

    if not dryrun_fl:
        job_script = Path(str(call_params['output_dir'] / f'melange{label}') + machine.scheduler.script_ext)
        job_script.touch()
        job_script.write_text(header + modules + body)
    else:
        job_script = header + modules + body

    return job_script


if __name__ == '__main__':
    submit_job()
