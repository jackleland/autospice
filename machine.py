from warnings import warn
import scheduler as sch
import math
import collections

SCHEDULERS = {
    'slurm': sch.Slurm(),
    'loadleveller': sch.Loadleveller(),
    'pbs': sch.PBS()
}


class Machine(object):
    """
    Base class for defining a machine with nodes, cpu-time limitations and an output
    file creation method.
    """

    def __init__(self, name, cpus_per_node, memory_per_node, max_nodes, max_job_time, scheduler_name, modules=None):
        self.name = name
        self.max_cpus_per_node = cpus_per_node
        self.memory_per_node = memory_per_node
        self.max_nodes = max_nodes
        self.max_job_time = max_job_time
        if scheduler_name.lower() not in SCHEDULERS:
            raise NotImplementedError('Specified queue type is not currently supported.')
        self.scheduler = SCHEDULERS[scheduler_name.lower()]
        self.modules = modules

    def get_submission_script_modules(self):
        if self.modules is not None and isinstance(self.modules, collections.Iterable):
            return '\n' + '\n'.join([f'module load {module}' for module in self.modules]) + '\n\n'
        else:
            return '\n# NO MODULES REQUIRED\n\n'

    def get_safe_job_time(self):
        if self.max_job_time is not None:
            if self.max_job_time > 1:
                return int(self.max_job_time * 0.9)
            elif self.max_job_time == 1:
                return 1
            else:
                warn('Cannot get safe job time as max job time is poorly defined.')
                return 0
        else:
            raise ValueError('Cannot get safe job time as max job time is not defined.')

    def get_n_jobs(self, requested_walltime, safe_job_time_fl=False):
        """
        Method to get the number of jobs required to fill the requested amount
        of walltime on a machine with a set maximum job running time.

        :param requested_walltime:  The requested amount of walltime for a
                                    simulation, which can be given as a string
                                    in the form 'hh:mm:ss', as an int dictating
                                    the number of hours, or as a float
                                    indicating the total number of seconds (i.e.
                                    from timedelta.total_seconds() )
        :param safe_job_time_fl:    (boolean) Boolean flag to determine whether
                                    to use the 'safe' maximum job time instead
                                    of the full maxmimum job time (as defined by
                                    the member variable self.max_job_time). The
                                    max 'safe' job time is calculated from the
                                    get_safe_job_time() method.
        :return:    (int) The total number of jobs to complete that much
                    walltime on the machine.

        """
        # If max_job_time is not defined then only a single job will be needed.
        if self.max_job_time is None:
            return 1

        if isinstance(requested_walltime, str):
            hrs, mins, secs = (int(quantity) for quantity in requested_walltime.split(':'))
            walltime_seconds = (hrs * 3600) + (mins * 60) + secs
        elif isinstance(requested_walltime, int):
            walltime_seconds = requested_walltime * 3600
        else:
            walltime_seconds = int(requested_walltime)

        # Use safe job time or max job time in seconds.
        if safe_job_time_fl:
            job_time_seconds = self.get_safe_job_time() * 3600
        else:
            job_time_seconds = self.max_job_time * 3600

        return math.ceil(walltime_seconds / job_time_seconds)

    def calc_nodes(self, cpus):
        # Check if number of processors is sensible for this machine
        nodes = math.ceil(cpus / self.max_cpus_per_node)
        cpus_per_node = self.check_nodes(cpus, nodes)
        return nodes, cpus_per_node

    def check_nodes(self, cpus, nodes):
        cpus_per_node = cpus // nodes
        cpus_per_node_remainder = cpus % nodes
        if cpus_per_node_remainder > 0:
            raise ValueError(f'Number of processors chosen does not divide equally between the nodes on {self.name}. \n'
                             'There must be an equal number of processors used on each node. \n')
        if cpus_per_node > self.max_cpus_per_node:
            raise ValueError(f'Number of processors requested does not fit onto {nodes} node(s) on {self.name}\n'
                             f'You will need to increase the number of nodes requested. \n')
        if cpus_per_node < self.max_cpus_per_node:
            print("WARNING: Inefficient number of processors chosen - you won't be fully utilising every node. Your \n"
                  "account may still be charged for all nodes occupied \n")

        if nodes > self.max_nodes:
            raise ValueError(f'Number of processors requested would require more nodes ({nodes}) than the maximum \n'
                             f'available on this machine ({self.max_nodes})\n')
        elif nodes == self.max_nodes:
            print('WARNING: Using maximum acceptable number of nodes on this machine. If you have any currently \n'
                  'running jobs this job will not be run until they have finished. \n')
        return cpus_per_node

    def get_isolated_node_distribution(self, cpus, nodes):
        minimum_cpus = (cpus - 1) // (nodes - 1)
        maximum_cpus = minimum_cpus + ((cpus - 1) % (nodes - 1))
        if minimum_cpus > self.max_cpus_per_node or maximum_cpus > self.max_cpus_per_node:
            raise ValueError(f'Number of processors required to isolate the first node does not fit onto {nodes} '
                             f'node(s) on {self.name}\n'
                             f'You will need to increase the number of nodes requested. \n')
        return minimum_cpus, maximum_cpus


marconi_skl = Machine('Marconi', 48, 177, 64, 24, "slurm")
marconi_skl_fuaspecial = Machine('Marconi', 48, 177, 64, 180, "slurm")
cumulus = Machine('Cumulus', 32, 512, 16, None, "pbs", modules=['ifort/2017.0.098', 'icc/2017.0.098'])
