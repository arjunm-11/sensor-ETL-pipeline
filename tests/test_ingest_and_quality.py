"""Unit tests for batching (pipeline/ingest.py) and check logic (pipeline/quality.py)."""
import gzip

import pytest

from pipeline.ingest import _iter_batches, grid_bounds, make_batch_id
from pipeline.quality import CheckResult, DataQualityError, raise_on_errors


# ---- batching ---------------------------------------------------------------

@pytest.mark.parametrize("line_no,expected", [(1, (1, 500)), (500, (1, 500)), (501, (501, 1000)), (1234, (1001, 1500))])
def test_grid_bounds(line_no, expected):
    assert grid_bounds(line_no, 500) == expected


def test_batch_id_is_deterministic_and_sorts_in_file_order():
    ids = [make_batch_id("data.txt.gz", *grid_bounds(n, 500_000)) for n in (1, 500_001, 2_000_001)]
    assert ids[0] == "data.txt.gz:000000001-000500000"
    assert ids == sorted(ids)


@pytest.fixture
def gz_file(tmp_path):
    path = tmp_path / "data.txt.gz"
    lines = [f"line{i} " for i in range(1, 8)]          # trailing space is data
    with gzip.open(path, "wt", encoding="latin-1", newline="") as f:
        f.write("\n".join(lines) + "\n")
    return path


def test_iter_batches_splits_on_grid_and_keeps_trailing_spaces(gz_file):
    batches = list(_iter_batches(gz_file, batch_size=3, first_line=1))
    assert [(s, e, len(r)) for s, e, r in batches] == [(1, 3, 3), (4, 6, 3), (7, 9, 1)]
    assert batches[0][2][0] == (1, "line1 ")              # only "\n" stripped


def test_iter_batches_resumes_from_first_line(gz_file):
    # Incremental mode restarts at the start of a grid batch.
    batches = list(_iter_batches(gz_file, batch_size=3, first_line=4))
    assert [b[0] for b in batches] == [4, 7]
    assert batches[0][2][0][0] == 4


# ---- check results ----------------------------------------------------------

def make(failed, checked=100, threshold=0.05, severity="warn"):
    return CheckResult("c", "clean", severity, checked, failed, threshold, "d")


@pytest.mark.parametrize("failed,passed", [(0, True), (5, True), (6, False)])
def test_threshold_is_inclusive_max_failure_fraction(failed, passed):
    assert make(failed).passed is passed


def test_zero_threshold_means_no_failures_allowed():
    assert make(0, threshold=0.0).passed
    assert not make(1, threshold=0.0).passed


def test_empty_check_passes_only_with_no_failures():
    assert make(0, checked=0).passed
    assert not make(1, checked=0).passed


def test_only_failed_error_checks_raise():
    raise_on_errors([make(50, severity="warn"), make(0, severity="error")])   # no raise
    with pytest.raises(DataQualityError, match="c"):
        raise_on_errors([make(1, threshold=0.0, severity="error")])
