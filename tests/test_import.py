import pytest
import numpy.testing as npt
import pdb


from odisi import read_tsv

DATA_FULL = read_tsv("tests/data/2023-09-05_test-run_ch1_full.tsv")


class TestMetadata:
    def test_channel(self):
        assert DATA_FULL.channel == 1

    def test_rate(self):
        assert DATA_FULL.rate == 1.04167

    def test_gage_pitch(self):
        assert DATA_FULL.gage_pitch == 0.65


class TestData:
    def test_data_x_full(self):
        x = DATA_FULL.x
        diff = (x[1] - x[0]) * 1e3
        npt.assert_almost_equal(diff, DATA_FULL.gage_pitch)

    def test_data_full(self):
        data = DATA_FULL.data[0, 5:10].to_numpy()[0]
        v = [-4.5, 3, -2.9, -6.8, -0.9]
        npt.assert_almost_equal(data, v)
