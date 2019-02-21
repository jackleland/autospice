import configparser
from warnings import warn
from pathlib import Path
from collections import OrderedDict
from pprint import pprint
from datetime import timedelta
import subprocess

import git
from humanfriendly import format_timespan
import click


CPUS_PER_NODE = {'marconi': 48}
EMAIL = {'tnichola': 'tegn500@york.ac.uk'}
SUPPORTED_MACHINES = ['marconi']


@click.command()
@click.argument('config_file', type=click.Path(exists=True))
def submit_job(config_file):
    """
    Reads a YAML-like configuration file, writes a job script, and submits a
    BOUT++ simulation job based on the options contained in the file.

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
    """

    # Read and parse the config file
    config_file = Path(config_file)
    config = configparser.ConfigParser()
    config.read(config_file)
    scheduler_opts, code_opts = config['scheduler'], config['code']

    # Set strings to bools if appropriate
    restart, append = False, False
    if code_opts['restart'].lower().strip() is 'true':
        code_opts['restart'] = True
    if code_opts['append'].lower().strip() is 'true':
        code_opts['append'] = True

    # TODO somehow determine the user from the environment the script is run in
    user, machine = scheduler_opts['user'], scheduler_opts['machine']
    if machine.lower() not in SUPPORTED_MACHINES:
        raise NotImplementedError("This script assumes you're submitting a "
                                  f"job on one of {SUPPORTED_MACHINES}")
    email = EMAIL[user]
    print(f"User {user} on machine {machine}")
    print(f"Job completion and error notifications will be sent to {email}")

    # Check if number of processors is sensible for this machine
    cpus = scheduler_opts['n_cpus']
    try:
        cpus = int(cpus)
    except:
        raise TypeError("Can't use a non-integer number of CPUs")
    nodes = calc_nodes(cpus, machine)

    output = Path(code_opts['output'])

    # Check input file exists
    input = Path(code_opts['input'])
    full_input = full(output / input)
    if not (output / input).is_file():
        raise FileNotFoundError(f"No input file found at {full_input}")

    # Check executable file exists
    executable = Path(code_opts['executable'])
    if not executable.is_file():
        raise FileNotFoundError(f"No executable file found at {executable}")

    git_check(executable)

    # TODO Check if input file contains a path to a valid equilibrium file

    # TODO Check input file specifies a sensible number of grid points

    if list(output.glob('*.nc')):
        warn("There are netCDF files already in the output directory which "
             "might get overwritten")

    if not output.is_dir():
        raise NotADirectoryError("output does not point to a valid directory")

    print_choices(scheduler_opts, code_opts, input, executable, cpus, nodes)

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

def calc_nodes(cpus, machine):
    # Check if number of processors is sensible for this machine
    cpus_per_node = CPUS_PER_NODE[machine]
    nodes = cpus // cpus_per_node
    remainder = cpus % cpus_per_node
    if remainder:
        nodes += 1
        warn("Inefficient number of processors chosen - you won't be fully "
             "utilising every node. Your account will also be charged for all "
             "nodes occupied!")
    return nodes


def git_check(executable_path):
    # Check if latest changes to executable have been committed
    repo = git.Repo(executable_path, search_parent_directories=True)
    if repo.is_dirty():
        warn("There are uncommitted changes to the executable code's git "
             "repository")

    # TODO Check if the executable has been compiled since the last commit


def print_choices(scheduler_opts, code_opts, full_input, executable, cpus, nodes):
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
    cpus_per_node = str(CPUS_PER_NODE[machine])
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
            f"#SBATCH --mail-user {EMAIL[scheduler_opts['user']]}\n"
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