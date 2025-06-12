import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import utils


def test_find_next_available_dir(tmp_path):
    base = tmp_path / "run"
    base.mkdir()
    # first available should append _1_
    next_dir = utils.find_next_available_dir(base)
    assert next_dir == base.parent / "run_1_"
    (base.parent / "run_1_").mkdir()
    next_dir2 = utils.find_next_available_dir(base)
    assert next_dir2 == base.parent / "run_2_"


def test_find_next_available_filename(tmp_path):
    base = tmp_path / "output.txt"
    base.touch()
    next_file = utils.find_next_available_filename(base)
    assert next_file == base.parent / "output_1_.txt"
