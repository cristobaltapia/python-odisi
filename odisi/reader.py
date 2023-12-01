import re
from pathlib import Path
import pdb

import numpy as np
import polars as pl
import polars.selectors as cs

from odisi.odisi import OdisiResult, OdisiGagesResult


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
    info = []

    with open(path, "r") as f:
        # Initialize counter for the lines
        n_skip = 1
        # Look for the end of the metadata block
        while True:
            s = f.readline()

            if s[:5] != "-----":
                # Append metadata to the list
                info.append(s.split(":"))
                n_skip += 1
            else:
                seg = f.readline().strip()
                if seg[:12] == "Gage/Segment":
                    n_skip += 2

                break

    # Determine if file has segments defined
    if n_skip == 31:
        with_gages = False
    else:
        with_gages = True

    # Initialize dictionary to store metadata
    metadata = {}

    for k in info:
        metadata[k[0].strip()] = k[1].strip()

    # Read data from optical sensor
    df = pl.read_csv(
        path,
        has_header=False,
        skip_rows=n_skip,
        skip_rows_after_header=2,
        separator="\t",
        try_parse_dates=True,
    )

    # Rename time column
    time = df.rename({df.columns[0]: "time"}).select(pl.col("time"))

    # Cast as floats
    data = df[:, 3:].with_columns(cs.matches(r"\d").cast(float))
    df = pl.concat([time, data], how="horizontal")

    # Get line number for the x-coordinate
    line_x = n_skip - 1 if with_gages else n_skip
    # Read the x-coordinate information
    x = pl.read_csv(
        path,
        skip_rows=line_x,
        skip_rows_after_header=0,
        n_rows=1,
        separator="\t",
    )

    x = x.select(cs.matches("[0-9]").cast(float)).to_numpy()[0]

    if with_gages:
        result = OdisiGagesResult(data=df, x=x, gages=[], metadata=metadata)
    else:
        result = OdisiResult(data=df, x=x, metadata=metadata)

    return result
