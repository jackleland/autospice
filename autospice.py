import configparser
from warnings import warn
from pathlib import Path
from collections import OrderedDict
from pprint import pprint
from datetime import timedelta
import subprocess
import machine as mch
import codes
import os
import shutil
from logger import Logger

from humanfriendly import format_timespan
import click

from utils import find_next_available_dir

SUPPORTED_MACHINES = {
    'marconi': mch.marconi_skl,
    'cumulus': mch.cumulus
}
# TODO: Auto-populate this with ast
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
    config_file = Path(config_file)
    config = configparser.ConfigParser()
    config.read(config_file)
    scheduler_opts, code_opts = config['scheduler'], config['code']

    # Check code specific options
    code_name = code_opts['code_name']

    if code_name not in SUPPORTED_CODES:
        raise NotImplementedError("This script only supports the use of certain codes."
                                  f"Currently implemented codes are: {list(SUPPORTED_CODES.keys())}")
    # Create SimulationCode object and use it to process the code specific config options
    sim_code = SUPPORTED_CODES[code_name]
    code_specific_opts = sim_code.process_config_options(config[code_name])

    # Check Scheduler Options
    # TODO somehow determine the user from the environment the script is run in
    job_name = scheduler_opts['job_name']
    user, machine_name = scheduler_opts['user'], scheduler_opts['machine']

    if machine_name.lower() not in SUPPORTED_MACHINES:
        raise NotImplementedError("This script assumes you're submitting a "
                                  f"job on one of {SUPPORTED_MACHINES}")
    print(f"User {user} on machine {machine_name}")
    machine = SUPPORTED_MACHINES[machine_name]

    email = scheduler_opts['email']
    print(f"Job completion and error notifications will be sent to {email}")

    # Check if number of processors is sensible for this machine
    cpus_tot = scheduler_opts['n_cpus']
    try:
        cpus_tot = int(cpus_tot)
    except:
        raise TypeError("Can't use a non-integer number of CPUs")
    nodes, cpus_per_node = machine.calc_nodes(cpus_tot)

    memory_req = int(scheduler_opts['memory'])
    if memory_req > machine.memory_per_node * nodes:
        # TODO: Memory should be able to be prioritised above maximising cpus_per_node
        warn(f"Requested amount of memory exceeds the maximum available on {machine_name}. With {nodes} nodes the "
             f"maximum amount of available memory is {machine.memory_per_node * nodes}GB, the job will be submitted "
             f"with this amount requested. To submit the job with {memory_req}GB of memory, you would require "
             f"{memory_req / machine.memory_per_node} nodes.")
        memory_req = machine.memory_per_node * nodes

    # TODO: verify string is in correct format
    walltime = scheduler_opts['walltime']
    n_jobs = machine.get_n_jobs(walltime, safe_job_time_fl=True)
    if n_jobs > 1:
        warn(f"Walltime requested {walltime} exceeds the maximum available walltime on {machine_name}. "
             f"The job will be split into {n_jobs} to complete successfully.")
        walltime = f"{machine.get_safe_job_time()}:00:00"

    # Check Code Options
    executable_dir = Path(code_opts['bin'])
    output_dir = Path(code_opts['output'])
    input_file = Path(code_opts['input'])
    # TODO: This is spice specific - change when upgrading generality
    restart_fl = code_specific_opts.getboolean('soft_restart') or code_specific_opts.getboolean('full_restart')

    # Change directory to spice bin
    print(f"Changing directory to {executable_dir}")
    # if not dryrun_fl:
    if executable_dir.resolve().is_dir():
        os.chdir(executable_dir)
    else:
        raise ValueError('The "bin" variable must be a valid directory with a spice binary in it.\n'
                         f'{executable_dir}')

    # Directory I/O for regular and restart runs. If regular 'spice' io, create directory; if restart, backup
    # directory before starting run.
    if not restart_fl:
        # If not restarting then check if the output folder exists already.
        if output_dir.exists() and output_dir.is_dir():
            warn(f"{output_dir} already exists, searching for next available "
                 "similar directory \n")
            output_dir = find_next_available_dir(output_dir)
        # Create output directory
        print(f"Using directory {output_dir}")
        if not dryrun_fl:
            os.mkdir(output_dir)
    elif output_dir.exists() and sim_code.is_code_output_dir(output_dir):
        # If restarting make a backup of the existing directory
        # TODO: This is SPICE specific - change when upgrading generality
        restart_backup = find_next_available_dir(Path(f"{output_dir}_atrestart"))
        print(f"Restarting {sim_code.name} run in directory {output_dir}, will place a"
              f"backup of start files in {restart_backup} \n")
        if not dryrun_fl:
            shutil.copy(output_dir, restart_backup)
    elif output_dir.exists() and not output_dir.is_dir():
        raise ValueError(f'Desired directory ({output_dir}) is not a {sim_code.name} directory.'
                         f'and therefore not restartable.')
    else:
        raise FileNotFoundError(f'No directory found to restart at {output_dir}')

    # Check input file exists
    if not input_file.is_file():
        print(os.listdir())
        raise FileNotFoundError(f"No input file found at {input_file}")
    sim_code.verify_input_file(input_file)

    # Check executable file exists
    executable = Path(code_opts['executable'])
    if not executable.is_file() and not dryrun_fl:
        raise FileNotFoundError(f"No executable file found at {executable}")

    # TODO: This has been temporarily removed as the syntax has changed from
    #  Tom's example script.
    # git_check(executable)

    print_choices(scheduler_opts, code_opts, executable_dir / input_file, executable_dir / output_dir,
                  executable_dir / executable, cpus_tot, nodes)
    sim_code.print_config_options(code_specific_opts)

    submission_params = {
        'job_name': job_name,
        'nodes': nodes,
        'cpus_per_node': cpus_per_node,
        'walltime': walltime,
        'out_log': output_dir / 'log.out',
        'err_log': output_dir / 'log.err',
        'queue_name': scheduler_opts['queue'],
        'memory': memory_req,
        'account': scheduler_opts['account'],
        'email': email,
        'email_events': machine.scheduler.default_email_settings
    }
    call_params = {
        'cpus_tot': cpus_tot,
        'executable': executable,
        'output_dir': output_dir,
        'input_file': input_file,
        'config_opts': code_specific_opts
    }
    if click.confirm('\nDo you want to continue?', default=True):
        # TODO: Running multiple subsequent jobs based on machine's max job time.

        # job_script = write_job_script(config_file.parent, scheduler_opts, code_opts, input_file, output_dir, nodes)
        job_script = write_job_script(submission_params, machine, sim_code, call_params)

        # Submit job script
        if not dryrun_fl:
            out = subprocess.check_output(['sbatch', str(job_script)])
            *rest, job_num = str(out, 'utf-8').split(' ')
            print(f"\nSubmitted job number {job_num}")

            if n_jobs > 1:
                job_script = write_job_script(submission_params, machine, sim_code, call_params, multi_submission=True)
                for i in n_jobs - 1:
                    out = subprocess.check_output(['sbatch', f'-d afterany:{job_num}', str(job_script)])
                    *rest, job_num = str(out, 'utf-8').split(' ')
                    print(f"\nSubmitted multisubmission {i}, job number {job_num}")

            logger = Logger()
            logger.update_log({
                'machine': machine_name,
                'job_number': job_num,
                'job_name': job_name,
                'input_file': input_file,
                'masala_config': config_file,
                'nodes': nodes,
                'total_cores': cpus_tot,
                'memory_req': memory_req,
                'wtime_req': walltime,
                'notes': ''
            })


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
    print(f"Will use {str(cpus)} cpus across {str(nodes)} nodes")

    hrs, min, sec = (int(quantity) for quantity in scheduler_opts['walltime'].split(':'))
    total_walltime = timedelta(hours=hrs, minutes=min, seconds=sec) * cpus
    print(f"Total CPU time requested is {format_timespan(total_walltime)}")


def full(path):
    return str(path.resolve())


def write_job_script(submission_params, machine, code, call_params, multi_submission=False):
    # TODO: Write to file and point to with path
    header = machine.scheduler.get_submission_script_header(submission_params)
    body = code.get_submission_script_body(machine, *call_params.values(), multi_submission=multi_submission)

    job_script = Path(str(call_params['output_dir'] / 'melange') + machine.scheduler.script_ext)
    job_script.touch()
    job_script.write_text(header + body)

    return job_script

# def write_job_script(config_dir, scheduler_opts, code_opts, input, output, nodes):
#
#     machine = scheduler_opts['machine']
#     cpus_per_node = str(SUPPORTED_MACHINES[machine].cpus_per_node)
#     name = scheduler_opts['name']
#     executable = Path(code_opts['executable'])
#
#     restart, append = '', ''
#     if code_opts['restart']:
#         restart = 'restart'
#     if code_opts['append']:
#         append = 'append'
#
#     text = (
#         "#!/bin/bash\n"
#         f"#SBATCH -J {scheduler_opts['name']}\n"
#         f"#SBATCH -N {str(nodes)}\n"
#         f"#SBATCH --tasks-per-node={cpus_per_node}\n"
#         f"#SBATCH -p {scheduler_opts['queue']}\n"
#         f"#SBATCH -t {scheduler_opts['walltime']}\n"
#         f"#SBATCH -A {scheduler_opts['account']}\n"
#         f"#SBATCH -o {full(output / name)}.out\n"
#         f"#SBATCH -e {full(output / name)}.err\n"
#         f"#SBATCH --mail-type END,FAIL\n"
#         f"#SBATCH --mail-user {scheduler_opts['email']}\n"
#         "export OMP_NUM_THREADS=1\n\n"
#
#         f"cd {full(executable.parent)}\n"
#         f"mpirun -np {scheduler_opts['n_cpus']} "
#         f"{full(executable)} {restart} {append} "
#         f"-f {code_opts['input']} "
#         f"-d {full(output)}"
#     )
#
#     job_script = Path(str(config_dir / name) + '.slurm')
#     job_script.touch()
#     job_script.write_text(text)
#
#     return job_script


if __name__ == '__main__':
    submit_job()
