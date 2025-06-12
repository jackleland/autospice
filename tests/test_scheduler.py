import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import scheduler


def test_get_submission_script_header():
    params = {
        "job_name": "test",
        "nodes": 1,
        "cpus_per_node": 2,
        "walltime": "00:10:00",
        "out_log": "out.log",
        "err_log": "err.log",
    }
    header = scheduler.Slurm().get_submission_script_header(params)
    assert header.splitlines() == [
        "#!/bin/bash",
        "#SBATCH -J test",
        "#SBATCH -N 1",
        "#SBATCH --tasks-per-node=2",
        "#SBATCH -t 00:10:00",
        "#SBATCH -o out.log",
        "#SBATCH -e err.log",
    ]
