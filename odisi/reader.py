import re
from pathlib import Path
import pdb

import numpy as np
import polars as pl
import polars.selectors as cs
from numpy.typing import ArrayLike

from odisi.odisi import OdisiGagesResult, OdisiResult


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
    with_gages = False

    with open(path, "r") as f:
        # Initialize counter for the lines
        n_meta = 1
        # Look for the end of the metadata block
        while True:
            s = f.readline()

            if s[:5] != "-----":
                # Append metadata to the list
                info.append(s.split(":"))
                n_meta += 1
            else:
                # Read next line after the end of the metadata to determine
                # whether the file contains gages/segments or not.
                seg = f.readline().strip()
                if seg[:12] == "Gage/Segment":
                    with_gages = True
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

    if with_gages:
        n_skip = n_meta + 3
    else:
        n_skip = n_meta + 2

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
    line_x = 3 if with_gages else 2

    # Read data for: x-coordinate, tare and gages/segments (if available)
    t = pl.read_csv(
        path,
        skip_rows=n_meta,
        n_rows=line_x,
        separator="\t",
        has_header=False,
    )

    x = t[-1, 3:].select(pl.all().cast(float)).to_numpy()[0]

    if with_gages:
        g = t[0, 3:].to_numpy()[0]
        gages = get_gages(g)
        result = OdisiGagesResult(data=df, x=x, gages=gages, metadata=metadata)
    else:
        result = OdisiResult(data=df, x=x, metadata=metadata)

    return result


def get_gages(x: ArrayLike) -> list:
    """Get the names and indices of gages.

    Parameters
    ----------
    x : ArrayLike
        The whole row with the names of gages and segments.

    Returns
    -------
    gages : list
        List with the names of the gages.

    """
    # Columns correponding to a segments have the following format: id[number]
    # Gages only conain the name (without the bracket + number). This the next
    # regular pattern will only find gages and will exclude segments.
    pattern_id = re.compile(r"(?>[\w ]+)(?!\[\d+\])")

    # Math each column name against the pattern until no match is found (the
    # gages are always at the beginning, followed by the segments).
    gages = []

    for k in x:
        m = pattern_id.match(k)
        if m:
            gages.append(m.group(0))
        else:
            break

    return gages

