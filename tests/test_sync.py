import pdb

import polars as pl
from numpy.testing import assert_almost_equal, assert_array_equal

from odisi import read_tsv


class TestInterpolation:
    def test_sync_time(self):
        data_full = read_tsv("tests/data/verification_data_ch1_full.tsv")
        data_time = pl.read_csv(
            "tests/data/verification_load.csv", try_parse_dates=True
        )

        time = data_time.select(["time [s]"])
        new_time = data_full.interpolate(time)
        r = [0.98, -3.9, 1.88, 2.74, 2.86]
        # Assert the correctness of the interpolation
        assert_almost_equal(new_time[2, 1:6].to_numpy()[0], r)
        # Assert the new rate
        assert data_full.rate == 0.2
