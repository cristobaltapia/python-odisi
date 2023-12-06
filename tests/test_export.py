import os

from odisi import read_tsv


class TestExport:
    def test_export(self, tmp_path):
        d = read_tsv("tests/data/verification_data_ch1_gages.tsv")
        d.export_segments_csv(prefix="test_export", path=tmp_path, with_time=True)
        # Check that the files were created
        assert os.path.exists(f"{tmp_path}/test_export_A1_data.csv")
        assert os.path.exists(f"{tmp_path}/test_export_A1_x.csv")
