import abc
import shutil
from pathlib import Path
import pprint as pp
from collections import OrderedDict
from flopter.spice.inputparser import InputParser
from utils import find_next_available_dir

LOG_PREFIX = 'log'


class SimulationCode(abc.ABC):
    """
    Abstract base class for storing code specific options and any necessary
    verification methods
    """
    SUBCLASS_COUNT = 0

    def __init__(self, name, config_file_labels, boolean_labels=None):
        self.name = name
        self.config_file_labels = config_file_labels
        if boolean_labels and set(self.config_file_labels).issuperset(set(boolean_labels)):
            self.boolean_labels = boolean_labels
        else:
            self.boolean_labels = set()
        SimulationCode.increment_counter()

    def process_config_options(self, config_opts):
        # Verify that the length of the config opts is correct
        if len(config_opts) != len(self.config_file_labels):
            raise ValueError('The options in the config file do not match those specified in '
                             'the code\'s definition. The config file should contain only these '
                             f'options: {self.config_file_labels} under the heading {self.name}')

        # Check that each value in the config file matches the defined values
        for defined_label in self.config_file_labels:
            if defined_label not in config_opts:
                raise ValueError(f'The option {defined_label} was not found in the config file.'
                                 f'The config file should contain all of these options: '
                                 f'{self.config_file_labels} under the heading {self.name}')
        for label in config_opts:
            if label not in self.config_file_labels:
                raise ValueError(f'An interloper option {label} was found in the config file.'
                                 f'The config file should contain only these options: '
                                 f'{self.config_file_labels} under the heading {self.name}')

        # Set strings to bools if appropriate
        if self.boolean_labels:
            for boolean_label in self.boolean_labels:
                try:
                    config_opts.getboolean(boolean_label)
                except ValueError:
                    raise ValueError(f'The boolean flag "{boolean_label}" is not set to a valid boolean value. \n'
                                     f'The current value is {config_opts[boolean_label]}.')
        return config_opts

    @abc.abstractmethod
    def print_config_options(self, config_opts):
        pass

    @abc.abstractmethod
    def get_command_line_args(self, config_opts):
        pass

    @abc.abstractmethod
    def get_submission_script_body(self, machine, call_params, multi_submission=False):
        pass

    @abc.abstractmethod
    def verify_input_file(self, input_file):
        pass

    @abc.abstractmethod
    def is_parameter_scan(self, input_file):
        pass

    @staticmethod
    @abc.abstractmethod
    def is_code_output_dir(directory):
        pass

    @abc.abstractmethod
    def directory_io(self, output_dir, config_opts, dryrun_fl):
        pass

    @classmethod
    def increment_counter(cls):
        cls.SUBCLASS_COUNT += 1


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

    def __init__(self):
        super().__init__('spice',
                         ('spice_version', 'verbose', 'soft_restart', 'full_restart'),
                         boolean_labels=('verbose', 'soft_restart', 'full_restart'))

    def process_config_options(self, config_opts):
        config_opts = super().process_config_options(config_opts)

        # Spice specific verification

        spice_version = config_opts['spice_version']
        if int(spice_version) not in [2, 3]:
            raise ValueError(f'spice_version given ({spice_version}) was not valid, must be either 2 or 3.')
        soft_restart, full_restart = config_opts.getboolean('soft_restart'), config_opts.getboolean('full_restart')
        if soft_restart and full_restart:
            raise ValueError('The soft and full reset flags were both set to true, please select only one if '
                             'you would like to restart a simulation. Full restart uses all available information'
                             'to restart the run (including diagnostics) and soft restart will only use '
                             'particle positions, velocities and the iteration count.')
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

        print('SPICE specific options are:')
        pp.pprint(OrderedDict(option_list))

    def get_command_line_args(self, config_opts):
        # Read restart mode in argument format
        restart_arg = self.get_restart_mode(config_opts)
        verbose_arg = '-v' if config_opts['verbose'] else None

        # Only return arguments which are set
        cl_args = [restart_arg, verbose_arg]
        return [arg for arg in cl_args if arg is not None]

    def get_submission_script_body(self, machine, call_params, multi_submission=False, safe_job_time_fl=True):
        # TODO: This should be replaced with either kwargs or an object
        cpus_tot = call_params['cpus_tot']
        executable = call_params['executable']
        executable_dir = call_params['executable_dir']
        output_dir = call_params['output_dir']
        input_file = call_params['input_file']
        config_opts = call_params['config_opts']

        precall_str = (
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

            f'cat {output_dir / LOG_PREFIX}.out >> {output_dir / LOG_PREFIX}.ongoing.out\n'
            f'cat {output_dir / LOG_PREFIX}.err >> {output_dir / LOG_PREFIX}.ongoing.err\n\n'

            f'BU_FOLDER="{output_dir}/backup_$(env TZ=GB date +"%Y%m%d-%H%M")"\n'
            'echo "Making backup of simulation data into $BU_FOLDER"\n'
            'mkdir "$BU_FOLDER"\n'
            f'cp {output_dir}/* $BU_FOLDER\n\n'
        )

        job_name = output_dir.name
        t_file = output_dir / f't-{job_name}'
        o_file = output_dir / f'{job_name}'

        config_file_args = self.get_command_line_args(config_opts)
        if multi_submission and not Spice.is_restart(config_opts):
            config_file_args.append('-c')

        if machine.max_job_time is not None and safe_job_time_fl:
            config_file_args.append(f'-l {machine.get_safe_job_time()}')

        mpirun_command = ' '.join(['mpirun', '-np', str(cpus_tot), str(executable_dir / executable),
                                   *config_file_args,
                                   '-o', str(o_file),
                                   '-i', str(input_file),
                                   '-t', str(t_file)
                                   ])
        call_str = (
            'echo ""\n'
            f'echo "executing: {mpirun_command}"\n'
            'echo ""\n'
            f'time {mpirun_command}\n'
        )
        return precall_str + call_str

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

    def verify_input_file(self, input_file):
        # TODO: Implement this!
        pass

    def is_parameter_scan(self, input_file):
        input_parser = InputParser(input_filename=input_file, read_comments_fl=False)
        scan_params = input_parser.get_scanning_params()
        if len(scan_params) > 1:
            raise ValueError('Attempting multi-dimensional parameter scan, not currently supported')
        return len(scan_params) == 1

    def get_scanning_parameters(self, input_file):
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
        # TODO: (2019-07-15) Implement multi-dimensional scans
        if len(scan_params) > 1:
            raise ValueError('Requested multi-dimensional parameter scan, not currently supported')
        return scan_params, input_parser

    @staticmethod
    def is_code_output_dir(directory):
        if not isinstance(directory, Path) and isinstance(directory, str):
            directory = Path(directory)
        if directory.is_dir():
            return len([f.name for f in list(directory.glob('*[!.][!2][!d].mat'))
                        if not f.name.startswith('t-')]) == 1 \
                   and len(list(directory.glob('t-*[!0-9][!0-9].mat'))) == 1 \
                   and len(list(directory.glob('t-*.mat'))) > 1
        else:
            return False

    def directory_io(self, output_dir, config_opts, dryrun_fl):
        # Directory I/O for regular and restart runs. If regular 'spice' io, create directory; if restart, backup
        # directory before starting run.
        restart_fl = self.is_restart(config_opts)
        if not restart_fl:
            # If not restarting then check if the output folder exists already.
            if output_dir.exists() and output_dir.is_dir():
                print(f"WARNING: {output_dir} already exists, searching for next available similar directory \n")
                output_dir = find_next_available_dir(output_dir)

            # Create output directory
            print(f"Using directory {output_dir} \n")
            if not dryrun_fl:
                output_dir.mkdir(parents=True)

        elif output_dir.exists() and self.is_code_output_dir(output_dir):
            # If restarting make a backup of the existing directory and run from there
            restart_dir = find_next_available_dir(Path(f"{output_dir}_restart"))
            print(f"Restarting {self.name} run in directory {restart_dir}, leave a backup of start files in "
                  f"{output_dir} \n")

            if not dryrun_fl:
                shutil.copy(output_dir, restart_dir)
            output_dir = restart_dir

        elif output_dir.exists() and not output_dir.is_dir():
            raise ValueError(f'Desired directory ({output_dir}) is not a {self.name} directory and therefore not '
                             f'restartable.\n')
        else:
            raise FileNotFoundError(f'No directory found to restart at {output_dir} \n')

        return output_dir
