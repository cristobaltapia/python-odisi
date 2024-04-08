from datetime import timedelta

from numpy.testing import assert_almost_equal, assert_array_equal

from odisi import read_tsv

DATA_GAGES = read_tsv("tests/data/verification_data_ch1_gages.tsv")


class TestMetadataGages:
    def test_time_shift(self):
        old_datetime = DATA_GAGES._data.get_column("time")[0]
        delta_t = timedelta(seconds=2)
        new_datetime = old_datetime + delta_t
        DATA_GAGES.shift_time(delta_t)
        assert DATA_GAGES.time[0] == new_datetime
