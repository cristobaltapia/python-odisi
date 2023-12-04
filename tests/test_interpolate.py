import pdb

import polars as pl
from numpy.testing import assert_almost_equal

from odisi import read_tsv


class TestInterpolation:
    def test_interp_time_dataframe(self):
        data_full = read_tsv("tests/data/verification_data_ch1_full.tsv")
        data_time = pl.read_csv(
            "tests/data/verification_load.csv", try_parse_dates=True
        )

        time = data_time.select(["time [s]"])[::2]
        new_time = data_full.interpolate(time)
        r = [-0.9, 0.8, 4.1, 4.0, 7.4333333]
        # Assert the correctness of the interpolation
        assert_almost_equal(new_time[3, 1:6].to_numpy()[0], r)
        # Assert the new rate
        assert data_full.rate == 0.4

    def test_interp_time_array(self):
        data_full = read_tsv("tests/data/verification_data_ch1_full.tsv")
        data_time = pl.read_csv(
            "tests/data/verification_load.csv", try_parse_dates=True
        )

        time = data_time.select(["time [s]"]).to_series().to_numpy()[::2]
        new_time = data_full.interpolate(time)
        r = [-0.9, 0.8, 4.1, 4.0, 7.4333333]
        # Assert the correctness of the interpolation
        assert_almost_equal(new_time[3, 1:6].to_numpy()[0], r)
        # Assert the new rate
        assert data_full.rate == 0.4

    def test_interp_time_relative(self):
        data_full = read_tsv("tests/data/verification_data_ch1_full.tsv")
        data_time = pl.read_csv(
            "tests/data/verification_load_relative_time.csv", try_parse_dates=True
        )

        time = data_time.select(["time [s]"]).to_series().to_numpy()[::2]
        new_time = data_full.interpolate(time, relative_time=True)
        r = [-0.9, 0.8, 4.1, 4.0, 7.4333333]
        # Assert the correctness of the interpolation
        assert_almost_equal(new_time[3, 1:6].to_numpy()[0], r)
        # Assert the new rate
        assert data_full.rate == 0.4
