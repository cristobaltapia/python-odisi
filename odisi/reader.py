import pdb
import re
from pathlib import Path
from typing import Iterable

import numpy as np
import polars as pl
import polars.selectors as cs

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
        separator="\t",
        try_parse_dates=True,
    )

    # Rename time column
    time = df.rename({df.columns[0]: "time"}).select(pl.col("time"))

    # Cast as floats
    data = df[:, 3:].with_columns(cs.all().cast(float))
    # We only use the time and data columns (not the strain and measurement ones)
    df = pl.concat([time, data], how="horizontal")

    # Get line number to read x-coordinate, tare and gages/segments information
    line_x = 3 if with_gages else 2

    # Read data for: x-coordinate, tare and gages/segments (if available)
    t = pl.read_csv(
        path,
        skip_rows=n_meta,
        n_rows=line_x,
        separator="\t",
        has_header=False,
    )

    # Get the x-coordinates
    x = t[-1, 3:].select(pl.all().cast(float)).to_numpy()[0]

    if with_gages:
        g = t[0, 3:].to_numpy()[0]
        # Get names and indices of gages
        gages = get_gages(g)
        # Get names and indices of segments
        segments = get_segments(g)

        result = OdisiGagesResult(
            data=df, x=x, gages=gages, segments=segments, metadata=metadata
        )
    else:
        result = OdisiResult(data=df, x=x, metadata=metadata)

    return result


def get_gages(x: Iterable[str]) -> dict[str, int]:
    """Get the names and indices of gages.

    Parameters
    ----------
    x : Iterable
        The row containing the names of gages and segments.

    Returns
    -------
    gages : dict[str, int]
        A dictionary mapping the name of the gages with the respective number
        of the column in the dataframe containing the sensor data.

    """
    # Columns corresponding to a segment have the following format: id[number]
    # Gages only contain the name (without the bracket + number). The next
    # regular pattern will only find gages and will exclude segments.
    pattern_id = re.compile(r"[\w ]+(?!\[\d+\])")
    # Initialize dictionary to map labels to column numbers
    gages = {}
    # Match each column name against the pattern until no match is found (the
    # gages are always at the beginning, followed by the segments).
    for ix, k in enumerate(x):
        m = pattern_id.match(k)
        if m:
            # The '+ 1' is needed to consider the first column used for the
            # timestamp.
            gages[m.group(0)] = ix + 1
        else:
            break

    return gages


def get_segments(x: Iterable) -> dict[str, tuple[int, int]]:
    """Get the names and indices of segments.

    Parameters
    ----------
    x : Iterable

    Returns
    -------
    segments : dict[str, tuple[int, int]]
        Dictionary mapping the labels to the corresponding (start, end) indices
        for each segment.

    """
    # Columns corresponding to a gage have the following format: id[number]
    # We will search for this format and extract the individual id's first.
    pattern_id = re.compile(r"(.*)\[\d+\]")

    # Match each column name against the pattern (returns an iterator of Match
    # objects)
    ch_match = (pattern_id.match(k) for k in x)

    # Now get the individual id's
    labels = np.unique([k.group(1) for k in ch_match if k]).tolist()

    segments = {}

    for s in labels:
        # Create a new pattern to find the indices of the current segment
        pattern_ix = re.compile(rf"{s}(?=\[\d+\])")
        # Match each column against the pattern
        match = (pattern_ix.match(k) for k in x)
        # Retrieve the indices corresponding to the current segment
        s_ix = [int(k) + 1 for k, m in enumerate(match) if m]
        segments[s] = (s_ix[0], s_ix[-1])

    return segments
