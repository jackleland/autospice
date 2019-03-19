from warnings import warn
import numpy as np


class Machine(object):
    """
    Base class for defining a machine with nodes, cpu-time limitations and an output
    file creation method.
    """
    QUEUE_EXTENSIONS = {
        'slurm': '.batch',
        'pbs': '.pbs'
    }

    def __init__(self, cpus_per_node, memory_per_node, max_nodes, max_job_time, queue_type):
        self.cpus_per_node = cpus_per_node
        self.memory_per_node = memory_per_node
        self.max_nodes = max_nodes
        self.max_job_time = max_job_time
        if queue_type not in self.QUEUE_EXTENSIONS:
            raise NotImplementedError('Specified queue type is not currently supported.')
        self.queue_type = queue_type

    def calc_nodes(self, cpus):
        # Check if number of processors is sensible for this machine
        nodes = cpus // self.cpus_per_node
        remainder = cpus % self.cpus_per_node
        if remainder:
            nodes += 1
            warn("Inefficient number of processors chosen - you won't be fully "
                 "utilising every node. Your account will also be charged for all "
                 "nodes occupied!")
        if nodes > self.max_nodes:
            raise ValueError(f'Number of nodes requested ({nodes}) is greater than the maximum '
                             f'available on this machine ({self.max_nodes})')
        elif nodes == self.max_nodes:
            warn('Using maximum acceptable number of nodes on this machine. '
                 'If you have any currently running jobs this job will not '
                 'be run until they have finished.')
        return nodes


marconi_skl = Machine(48, 182, 64, 24, "slurm")
cumulus = Machine(32, 512, 16, None, "pbs")
