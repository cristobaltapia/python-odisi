import re
from pathlib import Path

import numpy as np
import polars as pl
import polars.selectors as cs

from odisi.odisi import OdisiResult


def read_tsv(path: str | Path) -> OdisiResult:
    """Read the exported TSV file.

    Parameters
    ----------
    path : str
        Path to the file.

    Returns
    -------
    odisi : obj:`OdisiResult`

    """
    # TODO: determine if file has segments defined

    # Read relevant metadata
    with open(path, "r") as f:
        info = [f.readline().split(":") for _ in range(30)]

    # Initialize dictionary to store metadata
    metadata = {}

    for k in info:
        metadata[k[0].strip()] = k[1].strip()

    # Read data from optical sensor
    df = pl.read_csv(
        path,
        has_header=False,
        skip_rows=31,
        skip_rows_after_header=2,
        separator="\t",
        try_parse_dates=True,
    )
    # Rename time column
    time = df.rename({df.columns[0]: "time"}).select(pl.col("time"))
    # Cast as floats
    data = df[:, 3:].with_columns(cs.matches(r"\d").cast(float))
    df = pl.concat([time, data], how="horizontal")

    # Read the x-coordinate information
    x = pl.read_csv(
        path, skip_rows=31, skip_rows_after_header=0, n_rows=1, separator="\t"
    )
    x = x.select(cs.matches("[0-9]").cast(float)).to_numpy()[0]

    result = OdisiResult(data=df, x=x, metadata=metadata)

    return result
