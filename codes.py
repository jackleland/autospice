import abc
from pathlib import Path


class SimulationCode(abc.ABC):
    """
    Abstract base class for storing code specific options and any necessary verification methods
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
        if len(config_opts) != self.config_file_labels:
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
                if config_opts[boolean_label].lower().strip() is 'true':
                    config_opts[boolean_label] = True
        return config_opts

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

    @classmethod
    def increment_counter(cls):
        cls.SUBCLASS_COUNT += 1


class Spice(SimulationCode):
    """
    Implementation of Code class for Spice (2 & 3) with specific methods for processing config file options and
    verifying Spice input files.
    """

    def __init__(self):
        super().__init__('spice',
                         ('spice_version', 'verbose', 'soft_restart', 'full_restart'),
                         boolean_labels=('verbose', 'soft_restart', 'full_restart'))

    def process_config_options(self, config_opts):
        config_opts = super().process_config_options(config_opts)

        # Spice specific verification
        spice_version = config_opts['spice_version']
        if spice_version not in [2, 3]:
            raise ValueError(f'spice_version given ({spice_version}) was not valid, must be either 2 or 3.')
        soft_restart, hard_restart = config_opts['soft_restart'], config_opts['hard_restart']
        if soft_restart and hard_restart:
            raise ValueError('The soft and hard reset flags were both set to true, please select only one if '
                             'you would like to restart a simulation. Hard restart uses all available information'
                             'to restart the run (including diagnostics) and soft restart will only use '
                             'particle positions, velocities and the iteration count.')
        return config_opts

    def verify_input_file(self, input_file):
        # TODO: Implement this!
        pass

    def is_parameter_scan(self, input_file):
        # TODO: Implement this!
        pass

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
