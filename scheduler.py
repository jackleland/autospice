from abc import ABC


class Scheduler(ABC):
    """
    Abstract base class for a scheduler object, which will define a queueing
    system and allow the production of a submission script which adheres to the
    correct format. The default settings cover the regularly encountered fields
    on the Slurm scheduler.

    A Scheduler is defined by two key parts:
    (a) The 'get_submission_script_header()' method, which allows for the input
    of submission data and the output of a string, representing the submission
    parameters in proper format to be prepended to a submission script.
    (b) The 'parameter_mappings' dictionary, which maps from the submission
    parameters to their scheduler-specific argument names. This must contain all
    of the following input parameter -> argument mappings [by default]:

    Required for definition:            Required for writing submission script:
    - 'job_name'                        - Yes
    - 'nodes'                           - Yes
    - 'cpus_per_node'                   - Yes
    - 'walltime'                        - Yes
    - 'out_log'                         - Yes
    - 'err_log'                         - Yes
    - 'queue'                           - No
    - 'qos'                             - No
    - 'memory'                          - No
    - 'account'                         - No
    - 'email'                           - No
    - 'email_events'                    - No

    When calling the get_submission_script_header() method, it is not necessary
    to provide values for all of these input parameters, but all mappings must
    be defined for a complete Scheduler object to function. Also the
    parameter_mappings dicitonary is defined by the class member variables
    REQUIRED_PARAMS and OPTIONAL_PARAMS sets, which can be overridden upon
    implementation to change default behaviour.

    The Scheduler object also contains a name variable (not currently used) and
    a script_language variable, which determines which shell language the script
    is written in and therefore which shebang is included in the header.
    Currently the only supported languages are 'bash' and 'sh'.
    """
    REQUIRED_PARAMS = {
        'job_name',
        'nodes',
        'cpus_per_node',
        'walltime',
        'out_log',
        'err_log'
    }
    OPTIONAL_PARAMS = {
        'queue',
        'qos',
        'account',
        'memory',
        'email',
        'email_events'
    }
    SCRIPT_LANGS = {
        'bash': '#!/bin/bash',
        'sh': '#!/bin/sh'
    }

    def __init__(self, name, parameter_mappings, script_ext='.sh', script_lang='bash', default_email_settings='ALL'):
        self.name = name

        if not isinstance(parameter_mappings, dict):
            raise ValueError('parameter_mappings must be a dictionary')
        elif parameter_mappings.keys() != self.REQUIRED_PARAMS.union(self. OPTIONAL_PARAMS):
            raise ValueError('parameter_mappings must contain mappings for all of '
                             f'{self.REQUIRED_PARAMS.union(self. OPTIONAL_PARAMS)}')
        self.parameter_mappings = parameter_mappings

        if not isinstance(script_ext, str):
            raise TypeError('script_ext must be of type str')
        elif script_ext[0] is not '.':
            raise ValueError('script_ext must be a valid file extension, i.e. it must start with a "."')
        self.script_ext = script_ext

        if script_lang not in self.SCRIPT_LANGS:
            raise NotImplementedError('The selected submission script language '
                                      'is not currently supported. Currently '
                                      f'implemented languages are {self.SCRIPT_LANGS.keys()}')
        self.script_lang = script_lang
        self.shebang = self.SCRIPT_LANGS[script_lang]

        self.default_email_settings = default_email_settings

    def get_submission_script_header(self, submission_params, **kwargs):
        """
        The method in Scheduler which must be called to produce a submission
        script from the submission_param arg, which should be a dictionary with
        all required keys and a subset of the optional keys. These are found in
        the class variables REQUIRED_PARAMS and OPTIONAL_PARAMS, and are thus:

        required:
        - 'job_name'
        - 'nodes'
        - 'cpus_per_node'
        - 'walltime'
        - 'out_log'
        - 'err_log'

        optional:
        - 'queue'
        - 'qos'
        - 'memory'
        - 'account'
        - 'email'
        - 'email_events'

        :param submission_params:   (dict) mapping of the above keys to their
                                    values, to be inserted into a submission
                                    file header.
        :return:                    (string) submission script header to be
                                    prepended to a submission file matching the
                                    code execution commands

        """
        if not isinstance(submission_params, dict):
            raise TypeError('submission_params must be a dictionary')
        if not self.REQUIRED_PARAMS.issubset(submission_params.keys()):
            raise ValueError(f'submission_params must contain all of the required parameters {self.REQUIRED_PARAMS}. \n'
                             f'{self.REQUIRED_PARAMS - submission_params.keys()} was/were missing from submission '
                             f'params.')
        if not set(submission_params.keys()
                   - self.REQUIRED_PARAMS).issubset(self.OPTIONAL_PARAMS):
            raise ValueError(f'The only allowed optional parameters are {self.OPTIONAL_PARAMS}. You have input '
                             f'additional parameters \n'
                             f'{submission_params.keys() - self.REQUIRED_PARAMS - self.OPTIONAL_PARAMS}')

        # Construct list of parameter-value mappings joined by new-line symbols
        param_value_list = [self.shebang]
        param_value_list.extend([self.parameter_mappings[param].format(value)
                                 for param, value in submission_params.items()])
        # Append blank string so output ends in a new line
        param_value_list.append('')

        return '\n'.join(param_value_list)


#############################
#      Implementations      #
#############################

class Slurm(Scheduler):

    def __init__(self):
        parameter_mappings = {
            'job_name': '#SBATCH -J {}',
            'nodes': '#SBATCH -N {}',
            'cpus_per_node': '#SBATCH --tasks-per-node={}',
            'walltime': '#SBATCH -t {}',
            'out_log': '#SBATCH -o {}',
            'err_log': '#SBATCH -e {}',
            'queue': '#SBATCH -p {}',
            'qos': '#SBATCH -qos={}',
            'account': '#SBATCH -A {}',
            'memory': '#SBATCH --mem={}gb',
            'email': '#SBATCH --mail-user={}',
            'email_events': '#SBATCH --mail-type={}'
        }
        super().__init__('Slurm', parameter_mappings, script_ext='.slurm', script_lang='bash')


class PBS(Scheduler):
    """
    Implementation of Scheduler for the PBS queue submission system.
    """
    OPTIONAL_PARAMS = {
        'queue',
        'email',
        'email_events',
        'memory',
        'initial_dir'
    }

    def __init__(self):
        parameter_mappings = {
            'job_name':         '#PBS -N {}',
            'nodes':            '#PBS -l nodes={}',
            'cpus_per_node':    '#PBS -l ppn={}',
            'walltime':         '#PBS -l walltime={}',
            'out_log':          '#PBS -o {}',
            'err_log':          '#PBS -e {}',
            'initial_dir':      '#PBS -d {}',
            'queue':            '#PBS -q {}',
            'memory':           '#PBS -l pmem={}gb',
            'email':            '#PBS -M {}',
            'email_events':     '#PBS -m {}'
        }
        super().__init__('PBS', parameter_mappings, script_ext='.pbs', script_lang='bash')


class Loadleveller(Scheduler):
    """
    Implementation of Scheduler for the LoadLeveller queue submission system.
    """
    OPTIONAL_PARAMS = {
        'queue',
        'email',
        'email_events',
        'memory',
        'initial_dir'
    }

    def __init__(self):
        parameter_mappings = {
            'job_name':         '# @ job_name = {}',
            'nodes':            '# @ nodes = {}',
            'cpus_per_node':    '# @ cpus_per_node = {}',
            'walltime':         '# @ walltime = {}',
            'out_log':          '# @ output = {}',
            'err_log':          '# @ error = {}',
            'initial_dir':      '# @ initialdir = {}',
            'queue':            '# @ queue = {}',
            'memory':           '# @ requirements = (Memory >= {}gb)',
            'email':            '# @ notify_user = {}',
            'email_events':     '# @ notification = {}'
        }
        super().__init__('Loadleveller', parameter_mappings, script_ext='.pbs', script_lang='bash')

    def get_submission_script_header(self, submission_params, executable=None, arguments=None):
        script_header = super().get_submission_script_header(submission_params)
        return script_header + '# @ queue \n'
