import polars as pl

from odisi import read_tsv

DATA_FULL = read_tsv("tests/data/verification_data_ch1_full.tsv")
DATA_LOAD = pl.read_csv("tests/data/verification_load.csv")


class TestSync:
    def test_sync_time(self):
        time = DATA_LOAD.select(["time [s]"])
        new_time = DATA_FULL.sync(time)
