import configparser
from warnings import warn
from pathlib import Path
from collections import OrderedDict
from pprint import pprint
from datetime import timedelta
import subprocess
import machine
import codes
import os
import shutil
import glob

import git
from humanfriendly import format_timespan
import click


SUPPORTED_MACHINES = {
    'marconi': machine.marconi_skl,
    'cumulus': machine.cumulus
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
    user, machine = scheduler_opts['user'], scheduler_opts['machine']

    if machine.lower() not in SUPPORTED_MACHINES:
        raise NotImplementedError("This script assumes you're submitting a "
                                  f"job on one of {SUPPORTED_MACHINES}")
    print(f"User {user} on machine {machine}")

    email = scheduler_opts['email']
    print(f"Job completion and error notifications will be sent to {email}")

    # Check if number of processors is sensible for this machine
    cpus = scheduler_opts['n_cpus']
    try:
        cpus = int(cpus)
    except:
        raise TypeError("Can't use a non-integer number of CPUs")
    nodes = SUPPORTED_MACHINES[machine].calc_nodes(cpus)

    # Check Code Options
    executable_bin = Path(code_opts['bin'])
    output = Path(code_opts['output'])
    input_file = Path(code_opts['input'])
    restart_fl = code_opts['soft-restart'] or code_opts['hard-restart']

    # Change directory to spice bin
    print(f"Changing directory to {executable_bin}")
    if not dryrun_fl:
        os.chdir(executable_bin)

    restart_type = None

    # Directory I/O for regular and restart runs. If regular, create directory; if restart, backup
    # directory before starting run.
    if not restart_fl:
        # If not restarting then check if the output folder exists already.
        if output.exists() and output.is_dir():
            warn(f"{output} already exists, searching for next available "
                 "similar directory \n")
            output = find_next_available_dir(output)
        # Create output directory
        print(f"Using directory {output}")
        if not dryrun_fl:
            os.mkdir(output)
    elif output.exists() and sim_code.is_code_output_dir(output):
        # If restarting make a backup of the existing directory
        restart_type = '-c' if code_opts['hard-restart'] else '-r'
        restart_backup = find_next_available_dir(Path(f"{output}_atrestart"))
        print(f"Restarting {sim_code.name} run in directory {output}, will place a"
              f"backup of start files in {restart_backup} \n")
        if not dryrun_fl:
            shutil.copy(output, restart_backup)
    elif output.exists():
        raise ValueError(f'Desired directory ({output}) is not a {sim_code.name} directory '
                         f'and therefore not restartable.')
    else:
        raise FileNotFoundError(f'No directory found to restart at {output}')

    # Check input file exists
    if not input_file.is_file():
        raise FileNotFoundError(f"No input file found at {input_file}")

    # Check executable file exists
    executable = Path(code_opts['executable'])
    if not executable.is_file():
        raise FileNotFoundError(f"No executable file found at {executable}")

    # TODO: Validate input file?

    git_check(executable)

    print_choices(scheduler_opts, code_opts, code_specific_opts, input_file,
                  executable_bin / executable, cpus, nodes)

    if click.confirm('\nDo you want to continue?', default=True):

        job_script = write_job_script(config_file.parent, scheduler_opts,
                                      code_opts, input, output, nodes)

        # Submit job script
        out = subprocess.check_output(['sbatch', str(job_script)])
        *rest, job_num = str(out, 'utf-8').split(' ')
        print(f"\nSubmitted job number {job_num}")

        # Show job in queue
        print("\nCurrent status:\n")
        subprocess.call(f"squeue -u {user} -l", shell=True)


def find_next_available_dir(directory):
    i = 0
    dummy_dir = directory
    while dummy_dir.exists():
        i += 1
        dummy_dir = dummy_dir.parent / Path(f"{directory.stem}{i}")
    return dummy_dir


def git_check(executable_path):
    # Check if latest changes to executable have been committed
    repo = git.Repo(executable_path, search_parent_directories=True)
    if repo.is_dirty():
        warn("There are uncommitted changes to the executable code's git "
             "repository")

    # TODO Check if the executable has been compiled since the last commit


def print_choices(scheduler_opts, code_opts, code_specific_opts, full_input, executable, cpus, nodes):
    print("\nChosen options for simulation run are:")
    options = OrderedDict([('Run name', scheduler_opts['name']),
                           ('Input file path', str(full_input)),
                           ('Output directory', code_opts['output']),
                           ('Executable file path', str(executable)),
                           ('Queue', scheduler_opts['queue']),
                           ('Account', scheduler_opts['account']),
                           ('Walltime', scheduler_opts['walltime']),
                           ('Restart', code_opts['restart']),
                           ('Append', code_opts['append'])])
    pprint(options)

    print(f"Will use {str(cpus)} across {str(nodes)} nodes")

    hrs, min, sec = (int(quantity) for quantity
                     in scheduler_opts['walltime'].split(':'))
    total_walltime = timedelta(hours=hrs, minutes=min, seconds=sec) * cpus
    print(f"Total CPU time requested is {format_timespan(total_walltime)}")


def full(path):
    return str(path.resolve())


def write_job_script(config_dir, scheduler_opts, code_opts, input, output, nodes):

    machine = scheduler_opts['machine']
    cpus_per_node = str(SUPPORTED_MACHINES[machine].cpus_per_node)
    name = scheduler_opts['name']
    executable = Path(code_opts['executable'])

    restart, append = '', ''
    if code_opts['restart']:
        restart = 'restart'
    if code_opts['append']:
        append = 'append'

    text = ("#!/bin/bash\n"
            f"#SBATCH -J {scheduler_opts['name']}\n"
            f"#SBATCH -N {str(nodes)}\n"
            f"#SBATCH --tasks-per-node={cpus_per_node}\n"
            f"#SBATCH -p {scheduler_opts['queue']}\n"
            f"#SBATCH -t {scheduler_opts['walltime']}\n"
            f"#SBATCH -A {scheduler_opts['account']}\n"
            f"#SBATCH -o {full(output / name)}.out\n"
            f"#SBATCH -e {full(output / name)}.err\n"
            f"#SBATCH --mail-type END,FAIL\n"
            f"#SBATCH --mail-user {scheduler_opts['email']}\n"
            "export OMP_NUM_THREADS=1\n\n"

            f"cd {full(executable.parent)}\n"
            f"mpirun -np {scheduler_opts['n_cpus']} "
            f"{full(executable)} {restart} {append} "
            f"-f {code_opts['input']} "
            f"-d {full(output)}"
           )

    job_script = Path(str(config_dir / name) + '.slurm')
    job_script.touch()
    job_script.write_text(text)

    return job_script


if __name__ == '__main__':
    submit_job()
