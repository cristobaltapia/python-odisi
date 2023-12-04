import pdb
from datetime import datetime

import numpy as np
from numpy.testing import assert_almost_equal, assert_array_equal

from odisi import read_tsv

DATA_GAGES = read_tsv("tests/data/verification_data_ch1_gages.tsv")


class TestMetadataGages:
    def test_channel(self):
        assert DATA_GAGES.channel == 1

    def test_rate(self):
        assert DATA_GAGES.rate == 1.25

    def test_gage_pitch(self):
        assert DATA_GAGES.gage_pitch == 0.65


class TestData:
    def test_data_x_gage_pitch_full(self):
        x = DATA_GAGES.x
        diff = (x[100] - x[99]) * 1e3
        assert_almost_equal(diff, DATA_GAGES.gage_pitch)

    def test_data_x_full(self):
        x = DATA_GAGES.x
        r = [0.08, 16.8364, 9.20015, 5.6908, 2.0729]
        assert_almost_equal(x[0:5], r)

    def test_data_full(self):
        data = DATA_GAGES.data[0, 3:8].to_numpy()[0]
        v = [-4.9, -2, 0.6, 6.5, 0.5]
        assert_almost_equal(data, v)

    def test_time_full(self):
        t = DATA_GAGES.time
        real_time = [
            "2023-09-06 12:51:28.888946",
            "2023-09-06 12:51:29.689415",
            "2023-09-06 12:51:30.489885",
        ]
        r = np.array(
            [datetime.fromisoformat(k) for k in real_time],
            dtype=np.datetime64,
        )
        assert_array_equal(t[0:3], r)

