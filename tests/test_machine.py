import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import machine


def test_calc_nodes():
    nodes, cpus_per_node = machine.marconi_skl.calc_nodes(96)
    assert nodes == 2
    assert cpus_per_node == 48
