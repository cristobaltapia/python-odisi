import pdb
from datetime import datetime

import numpy as np
from numpy.testing import assert_almost_equal, assert_array_equal

from odisi import read_tsv

DATA_FULL = read_tsv("tests/data/verification_data_ch1_full.tsv")


class TestMetadataFull:
    def test_channel(self):
        assert DATA_FULL.channel == 1

    def test_rate(self):
        assert DATA_FULL.rate == 1.25

    def test_gage_pitch(self):
        assert DATA_FULL.gage_pitch == 0.65


class TestData:
    def test_data_x_gage_pitch_full(self):
        x = DATA_FULL.x
        diff = (x[1] - x[0]) * 1e3
        assert_almost_equal(diff, DATA_FULL.gage_pitch)

    def test_data_x_full(self):
        x = DATA_FULL.x
        r = [0.08, 0.08065, 0.0813, 0.08195, 0.0826]
        assert_almost_equal(x[0:5], r)

    def test_data_full(self):
        data = DATA_FULL.data[0, 1:6].to_numpy()[0]
        v = [3.7, -5.5, 2.8, 2.9, -2.3]
        assert_almost_equal(data, v)

    def test_time_full(self):
        t = DATA_FULL.time
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
