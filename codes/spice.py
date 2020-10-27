import pprint as pp
from collections import OrderedDict
from codes.core import SimulationCode
from flopter.spice import utils as sput
from flopter.spice.inputparser import InputParser
from utils import find_next_available_dir


class Spice(SimulationCode):
    """
    Implementation of Code class for Spice (2 & 3) with specific methods for processing config file options and
    verifying Spice input files.

    """
    RESTART_MODE_FORMATS = {
        'bool': (False, True, True),
        'arg': (None, '-r', '-c'),
        'short': (None, 's', 'f'),
        'long': (None, 'Soft', 'Full'),
        'desc': (
            'No restart',
            'Restart with particle information',
            'Restart with particle information and diagnostics'
        )
    }
    VERSION_LOG_PERCENTAGE_COLS = {
        2: '$1',
        3: '$2',
    }

    def __init__(self):
        super().__init__('spice',
                         ('spice_version', 'verbose', 'soft_restart', 'full_restart'),
                         optional_config_labels=('time_limit', ),
                         boolean_config_labels=('verbose', 'soft_restart', 'full_restart'))
        self.version = None

    def process_config_options(self, config_opts):
        config_opts = super().process_config_options(config_opts)

        # Spice specific verification

        spice_version = int(config_opts['spice_version'])
        if spice_version not in [2, 3]:
            raise ValueError(f'spice_version given ({spice_version}) was not valid, must be either 2 or 3.')
        self.version = spice_version
        soft_restart, full_restart = config_opts.getboolean('soft_restart'), config_opts.getboolean('full_restart')
        if soft_restart and full_restart:
            raise ValueError('The soft and full reset flags were both set to true, please select only one if '
                             'you would like to restart a simulation. Full restart uses all available information'
                             'to restart the run (including diagnostics) and soft restart will only use '
                             'particle positions, velocities and the iteration count.')

        if 'time_limit' in config_opts:
            try:
                time_limit = int(config_opts['time_limit'])
                if not time_limit > 0:
                    raise ValueError()
            except ValueError:
                raise ValueError('The "time_limit" optional code config option must be a positive, integer number '
                                 'of hours.')
            print(
                f'WARNING: You have specified a hard time limit on spice of {time_limit}hrs. This will override the \n'
                f'time set by --safe_job_time_fl.'
            )

        return config_opts

    def print_config_options(self, config_opts):
        restart_type = self.get_restart_mode(config_opts, rm_format='short')

        # Always print version and verbosity, print restart type if run is a restart
        option_list = [
            ('Spice Version', config_opts['spice_version']),
            ('Verbose', config_opts['verbose'])
        ]
        if restart_type is not None:
            option_list.append(('Restart type', restart_type))

        if 'time_limit' in config_opts:
            option_list.append(('Spice time limit', config_opts['time_limit'] + ' hr(s)'))

        print('SPICE specific options are:')
        pp.pprint(OrderedDict(option_list))

    def get_command_line_args(self, config_opts):
        # Read restart mode in argument format
        restart_arg = self.get_restart_mode(config_opts)
        verbose_arg = '-v' if config_opts['verbose'] else None
        time_limit = f'-l {int(config_opts["time_limit"])}' if 'time_limit' in config_opts else None

        # Only return arguments which are set
        cl_args = [restart_arg, verbose_arg, time_limit]
        return [arg for arg in cl_args if arg is not None]

    def get_submission_script_body(self, machine, call_params, multi_submission=False, safe_job_time_fl=True,
                                   backup_fl=True, spice_version=None):
        # TODO: This should be replaced with either kwargs or an object
        cpus_tot = call_params['cpus_tot']
        executable = call_params['executable']
        executable_dir = call_params['executable_dir']
        output_dir = call_params['output_dir']
        input_file = call_params['input_file']
        config_opts = call_params['config_opts']

        if spice_version not in self.VERSION_LOG_PERCENTAGE_COLS:
            if self.version is not None:
                spice_version = self.version
            else:
                spice_version = 2
        version_log_percent_col = self.VERSION_LOG_PERCENTAGE_COLS[spice_version]

        # TODO: (2020-04-24) This should probably be read in from an external bash file as opposed to being hardcoded
        precall_str = (
            'source $HOME/.bashrc\n'

            '\necho "Date is: $(env TZ=GB date)"\n'
            'echo "MPI version is: "\n'
            'echo ""\n'
            'mpirun --version\n'
            'echo ""\n'
            f'echo "Changing directory to {executable_dir}"\n'
            f'cd {executable_dir}\n\n'

            'if [ $(ulimit -s) != "unlimited" ]; then\n'
            '\techo "ulimit is:"\n'
            '\tulimit -s\n\n'

            '\techo ""\n'
            '\tulimit -s unlimited\n'
            '\techo "new ulimit is:"\n'
            '\tulimit -s\n'
            '\techo ""\n'
            'fi\n\n'
        )

        job_name = output_dir.name
        t_file = output_dir / f't-{job_name}'
        o_file = output_dir / f'{job_name}'

        config_file_args = self.get_command_line_args(config_opts)
        if multi_submission and not Spice.is_restart(config_opts):
            config_file_args.append('-c')

        if 'time_limit' not in config_opts and machine.max_job_time is not None and safe_job_time_fl:
            config_file_args.append(f'-l {machine.get_safe_job_time()}')

        # TODO: This could be passed here from a scheduler method, would make it a bit more modular.
        if 'node_dist_string' in call_params:
            # Make the run command use srun's arbitrary mode to distribute tasks to nodes non-equally
            run_command = ' '.join([
                'srun', '-n', str(cpus_tot),
                '-m', 'arbitrary',
                '-w', f'`{executable_dir / "arbitrary.pl"} {call_params["node_dist_string"]}`',
            ])
        else:
            run_command = ' '.join(['mpirun', '-np', str(cpus_tot)])
        spice_command = ' '.join([
            str(executable_dir / executable),
            *config_file_args,
            '-o', str(o_file),
            '-i', str(input_file),
            '-t', str(t_file)
        ])

        call_str = (
            'echo ""\n'
            f'echo "executing: {run_command} {spice_command}"\n'
            'echo ""\n'
            f'time {run_command} {spice_command}\n\n'
        )

        if spice_version == 3:
            stitcher_command = ' '.join([
                str(executable_dir / 'stitcher.bin'),
                '-i', str(input_file),
                '-t', str(t_file),
                '-n', str(cpus_tot)
            ])

            # Append a call to stitcher if using Spice-3
            call_str += (
                'echo ""\n'
                f'echo "executing: {stitcher_command}"\n'
                'echo ""\n'
                f'{stitcher_command}\n\n'
            )

        postcall_str = (
            f'\n\nsleep 600 \n'
            f'cat {output_dir / self.LOG_PREFIX}.out >> {output_dir / self.LOG_PREFIX}.ongoing.out\n'
            f'cat {output_dir / self.LOG_PREFIX}.err >> {output_dir / self.LOG_PREFIX}.ongoing.err\n\n'

            f'BU_FOLDER="{output_dir}/backup_$(env TZ=GB date +"%Y%m%d-%H%M")"\n'
            'echo "Making backup of simulation data into $BU_FOLDER"\n'
            'mkdir "$BU_FOLDER"\n'
            f"rsync -azvp --exclude='backup*' {t_file}.mat $BU_FOLDER\n"
            f"rsync -azvp --exclude='backup*' --exclude='*.mat' --exclude='*ongoing*' {output_dir}/* $BU_FOLDER\n"
        )
        if backup_fl:
            postcall_str += f"rsync -azvp --exclude='backup*' {output_dir}/* $BU_FOLDER\n"

        # Cancel all subsequent jobs if it looks like the simulation has finished
        postcall_str += (
            "\n"
            f"if (( $(cat {output_dir}/log.ongoing.out | grep '% ' | tail -n 1"
            f" | awk '{{print {version_log_percent_col}}}') >= 99 ))\n"
            "then \n"
            f"\tscancel $(cat {output_dir}/jobs.txt)\n"
            "fi\n"

        )

        return precall_str + call_str + postcall_str

    @classmethod
    def get_restart_mode(cls, config_opts, rm_format='arg'):
        # Verify rm_format is a valid option
        if rm_format not in cls.RESTART_MODE_FORMATS:
            raise ValueError(f'The format requested ({rm_format}) is not supported')

        # Get printable version of restart mode
        if config_opts.getboolean('full_restart'):
            restart_type = 2
        elif config_opts.getboolean('soft_restart'):
            restart_type = 1
        else:
            restart_type = 0

        return cls.RESTART_MODE_FORMATS[rm_format][restart_type]

    @classmethod
    def is_restart(cls, config_opts):
        return cls.get_restart_mode(config_opts, rm_format='bool')

    def verify_input_file(self, input_file, call_params, print_fl=True):
        input_parser = InputParser(input_filename=input_file)

        if print_fl:
            print('Verifying input file...')

        # TODO: might be more sensible to have this in input parser?
        if self.version == 3:
            geometry = input_parser['geom']
            for size_dim in ['Lx', 'Ly', 'Lz']:
                # Check simulation window dimensions are all powers of 2
                size = int(geometry[size_dim])
                if (size & (size - 1) != 0) or size == 0:
                    raise ValueError(
                        f'Invlaid simulation window size given ({size_dim}={size})\n'
                        f'Must be a power of 2 in 3D simulations.'
                    )

            # Check the decomposition is valid
            decomp_total = int(geometry['decompose_x']) * int(geometry['decompose_y'])
            if (decomp_total & (decomp_total - 1) != 0) or decomp_total == 0:
                raise ValueError(
                    f'Invalid x-y decomposition given (no. of decomposition areas = {decomp_total})\n'
                    f'Must be a power of 2 in 3D simulations.'
                )
            if decomp_total != call_params['cpus_tot']:
                raise ValueError(
                    f'Invalid x-y decomposition given (no. of decomposition areas = {decomp_total})\n'
                    f'Must equal the number of cpus requested ({call_params["cpus_tot"]}).'
                )

            # Check species properly set
            no_species = int(input_parser['num_spec']['no_species'])
            for i in range(no_species):
                species_sect = input_parser[f'specie{i}']
                if int(species_sect['mpi_rank']) != -1:
                    raise ValueError(f'Invalid mpi_rank on species {species_sect["name"]}, must be set to -1')
        # TODO: Could also check that the no_{section} values all equal the number of those sections

        if print_fl:
            print('...Input file verified successfully!\n')

    def is_parameter_scan(self, input_file):
        input_parser = InputParser(input_filename=input_file, read_comments_fl=False)
        scan_params = input_parser.get_scanning_params()
        return len(scan_params) >= 1

    @staticmethod
    def get_scanning_parameters(input_file):
        """
        Method for returning information about the parameters specified to be
        scanned. Returns a list of dictionaries with four entries for each
        scanning parameter, of the form:
        {
            'section':      [string] name of section where parameter is
            'parameter':    [string] name of parameter being scanned
            'values':       [list] parameter values to be scanned
            'length':       [int] length of the parameter scan, i.e. how many
                            values in the list of parameters
        }

        :return:    list of dicts containing the above data for each parameter

        """
        input_parser = InputParser(input_filename=input_file)
        scan_params = input_parser.get_scanning_params()
        return scan_params, input_parser

    @staticmethod
    def is_code_output_dir(directory):
        return sput.is_code_output_dir(directory)

    def directory_io(self, output_dir, config_opts, dryrun_fl, restart_copy_mode=0, print_fl=True):
        # Directory I/O for regular and restart runs. If regular 'spice' io, create directory; if restart, backup
        # directory before starting run.
        restart_fl = self.is_restart(config_opts)
        if not restart_fl:
            # If not restarting then check if the output folder exists already.
            if output_dir.exists() and output_dir.is_dir():
                if print_fl:
                    print(f"WARNING: {output_dir} already exists, searching for next available similar directory \n")
                output_dir = find_next_available_dir(output_dir)

            # Create output directory
            if print_fl:
                print(f"Using directory {output_dir} \n")
            if not dryrun_fl:
                output_dir.mkdir(parents=True)

        elif output_dir.exists() and self.is_code_output_dir(output_dir):
            output_dir = self.copy_on_restart(output_dir, dryrun_fl, restart_copy_mode)

        elif output_dir.exists() and output_dir.is_dir():
            if print_fl:
                print(f"WARNING: Directory {output_dir} doesn't look like a {self.name} simulation output folder.\n"
                      f"Will continue anyway.\n")
            output_dir = self.copy_on_restart(output_dir, dryrun_fl, restart_copy_mode)

        elif output_dir.exists() and not output_dir.is_dir():
            raise ValueError(f'Desired directory ({output_dir}) is not a {self.name} directory and therefore not '
                             f'restartable.\n')
        else:
            raise FileNotFoundError(f'No directory found to restart at {output_dir} \n')

        return output_dir