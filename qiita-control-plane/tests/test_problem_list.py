import pytest

from qiita_control_plane.problem_list import capped


@pytest.mark.parametrize(
    ("n", "expected"),
    [
        (2, ["0", "1"]),
        (3, ["0", "1", "2"]),
        (5, ["0", "1", "2", "and 2 more"]),
    ],
)
def test_capped_lists_under_at_and_over_the_cap(n, expected):
    assert capped([str(i) for i in range(n)], 3) == expected
