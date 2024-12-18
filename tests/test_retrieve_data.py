from datetime import datetime

import pytest
from numpy.testing import assert_almost_equal, assert_array_equal

from odisi import read_tsv

DATA_GAGES = read_tsv("tests/data/verification_data_ch1_gages.tsv")


class TestGages:
    def test_gage_data(self):
        p = DATA_GAGES.get_gage("Start").to_series()
        assert p[0] == 3.7

    def test_gage_time(self):
        p = DATA_GAGES.get_gage("Start", with_time=True)
        assert p[0, 1] == 3.7
        assert p[0, 0] == datetime.fromisoformat("2023-09-06 12:51:28.888946")

    def test_gages(self):
        p = DATA_GAGES.gages
        r = [
            "Start",
            "End",
            "A1s",
            "A1e",
            "A2s",
            "A2e",
            "B1s",
            "B1e",
            "B2s",
            "B2e",
            "All Gage",
            "A",
            "B",
        ]
        assert_array_equal(p, r)


class TestSegments:
    def test_segments(self):
        p = DATA_GAGES.segments
        r = ["A1", "A2", "All Gages", "B1", "B2"]
        assert_array_equal(p, r)

    def test_segment_data(self):
        p, x = DATA_GAGES.get_segment("B2")
        d = p.to_numpy()[0, -5:]
        r = [-1.3, -3, -0.9, -2.4, -0.2]
        assert_array_equal(d, r)
        assert x.shape[0] == p.shape[1]

    def test_segment_time(self):
        p, x = DATA_GAGES.get_segment("All Gages", with_time=True)
        assert p[0, 1] == 3.7
        assert p[0, 0] == datetime.fromisoformat("2023-09-06 12:51:28.888946")

    def test_x_coordinate_along_sensor(self):
        p, x = DATA_GAGES.get_segment("B2", x_along_sensor=True)
        d = p.to_numpy()[0, -5:]
        r = [-1.3, -3, -0.9, -2.4, -0.2]
        assert_array_equal(d, r)
        assert x[0] == 13.1132


class TestErrors:
    def test_error_bad_gage_label(self):
        with pytest.raises(KeyError) as excinfo:
            DATA_GAGES.get_gage("not a label")

        assert str(excinfo.value) == "'The given gage label does not exist.'"
