from datetime import datetime

import polars as pl
import pytest
from numpy.testing import assert_almost_equal

from odisi import read_tsv


@pytest.fixture
def luna_data():
    return read_tsv("tests/data/verification_data_ch1_full.tsv")


class TestInterpolation:
    def test_interp_time_dataframe(self, luna_data):
        data_time = pl.read_csv(
            "tests/data/verification_load.csv", try_parse_dates=True
        )

        time = data_time.select(["time [s]"])[::2]
        luna_data.interpolate(time)
        interp_data = luna_data.data
        r = [-0.9, 0.8, 4.1, 4.0, 7.4333333]
        # Assert the correctness of the interpolation
        assert_almost_equal(interp_data[3, 1:6].to_numpy()[0], r)
        # Assert the new rate
        assert luna_data.rate == 0.4

    def test_interp_time_array(self, luna_data):
        data_time = pl.read_csv(
            "tests/data/verification_load.csv", try_parse_dates=True
        )

        time = data_time.select(["time [s]"]).to_series().to_numpy()[::2]
        luna_data.interpolate(time)
        interp_data = luna_data.data
        r = [-0.9, 0.8, 4.1, 4.0, 7.4333333]
        # Assert the correctness of the interpolation
        assert_almost_equal(interp_data[3, 1:6].to_numpy()[0], r)
        # Assert the new rate
        assert luna_data.rate == 0.4

    def test_interp_time_relative(self, luna_data):
        data_time = pl.read_csv(
            "tests/data/verification_load_relative_time.csv", try_parse_dates=True
        )

        time = data_time.select(["time [s]"]).to_series().to_numpy()[::2]
        luna_data.interpolate(time, relative_time=True)
        interp_data = luna_data.data
        r = [-0.9, 0.8, 4.1, 4.0, 7.4333333]
        # Assert the correctness of the interpolation
        assert_almost_equal(interp_data[3, 1:6].to_numpy()[0], r)
        # Assert the new rate
        assert luna_data.rate == 0.4

    def test_interp_signal_time_relative(self, luna_data):
        data_time = pl.read_csv(
            "tests/data/verification_load_relative_time.csv", try_parse_dates=True
        )

        new_signal = luna_data.interpolate_signal(
            data_time, time="time [s]", relative_time=True
        )
        r = [0.00265, 0.001765, -0.00054, 0.000285, -0.004265]
        # Assert the correctness of the interpolation
        assert_almost_equal(new_signal[:5, 1], r)

    def test_interp_signal(self, luna_data):
        signal = pl.read_csv("tests/data/verification_load.csv", try_parse_dates=True)

        new_signal = luna_data.interpolate_signal(data=signal, time="time [s]")
        r = [0.00265, 0.001765, -0.00054, 0.000285, -0.004265]
        # Assert the correctness of the interpolation
        assert_almost_equal(new_signal[:5, 1].to_numpy(), r)

    def test_interp_time_array_clip(self, luna_data):
        data_time = pl.read_csv(
            "tests/data/verification_load.csv", try_parse_dates=True
        )

        time = data_time.select(["time [s]"])[::2]
        new_time = luna_data.interpolate(time, clip=True)
        interp_data = luna_data.data
        r = [-0.9, 0.8, 4.1, 4.0, 7.4333333]
        # Assert the correctness of the interpolation
        assert_almost_equal(interp_data[3, 1:6].to_numpy()[0], r)
        # Assert the new rate
        assert new_time[-1, 0] == datetime.fromisoformat("2023-09-06T12:54:08.088946")

    def test_interp_signal_clip(self, luna_data):
        signal = pl.read_csv("tests/data/verification_load.csv", try_parse_dates=True)

        new_signal = luna_data.interpolate_signal(
            data=signal, time="time [s]", clip=True
        )
        r = datetime.fromisoformat("2023-09-06T12:54:08.182396")
        # Assert the correctness of the interpolation
        assert new_signal[-1, 0] == r
