import numpy as np
import polars as pl
from numpy.typing import ArrayLike, NDArray


class OdisiResult:

    """Contains the data from the experiment.

    Attributes
    ----------
    gages : list[str]
        A list containing the name of each gage.
    segments : list[str]
        A list containing the name of each segment.
    data : obj:`DataFrame`
        A dataframe with the data of the experiment.
    x : ArrayLike
        The measurement positions along the sensor.
    metadata : dict
        Dictionary containing the metadata of the experiment.
    channel : int
        Number of the channel.
    rate : float
        Measurement rate in Hz.


    """

    def __init__(self, data, x, metadata):
        self.data: pl.DataFrame = data
        self.gages: list = []
        self.segments: list = []
        self._x: NDArray = x
        self._channel: int = int(metadata["Channel"])
        self._rate: float = float(metadata["Measurement Rate per Channel"][:-3])
        self._gage_pitch: float = float(metadata["Gage Pitch (mm)"])

    @property
    def x(self) -> NDArray:
        return self._x

    @property
    def channel(self):
        return self._channel

    @property
    def rate(self):
        return self._rate

    @property
    def gage_pitch(self):
        return self._gage_pitch

    @property
    def time(self) -> NDArray:
        return self.data.select(pl.col("time")).to_numpy().flatten()

    def gage(self, name: str):
        """Get data corresponding to the given gauge.

        Parameters
        ----------
        name : TODO

        Returns
        -------
        TODO

        """
        pass

    def segment(self, name: str):
        """Get data corresponding to the given segment.

        Parameters
        ----------
        name : TODO

        Returns
        -------
        TODO

        """
        pass

    def reverse_segment(self, name: str):
        """Reverse the direction of the segment.

        Parameters
        ----------
        name : TODO

        Returns
        -------
        TODO

        """
        pass

    def sync(self, time: ArrayLike):
        """TODO: Docstring for sync.

        Parameters
        ----------
        time : TODO

        Returns
        -------
        TODO

        """
        # Update self._rate
        pass


class OdisiGagesResult(OdisiResult):

    """Docstring ."""

    def __init__(self, segments):
        """TODO: to be defined.

        Parameters
        ----------
        segments : TODO


        """
        self._segments = segments

    def _split_gages(self):
        """TODO: Docstring for _split_gages.
        Returns
        -------
        TODO

        """
        # Get the names of the different segments (gauges)
        columns = df_channel.columns

        # Columns correponding to a gauge have the following format: id[number]
        # We will search for this format and extract the individual id's first.
        pattern_id = re.compile(r"(.*)\[\d+\]")

        # Math each column name against the pattern (returns an iterator of Match
        # objects)
        ch_match = (pattern_id.match(k) for k in columns)

        # Now get the indivudual id's
        ch_ids = np.unique([k.group(1) for k in ch_match if k])

        # Store each gage in a separate dataframe
        data_gages: dict[str, pl.DataFrame] = dict()

        for gage in ch_ids:
            # Get the columns corresponding to the current gage
            pattern_gage = re.compile(rf"{gage}\[\d+\]")
            gage_cols = [k for k in df_channel.columns if pattern_gage.match(k)]

            # Convert the x-coord data to float64.
            coords_i = x_coords[:, gage_cols].with_columns(
                pl.all().cast(pl.Float64, strict=False)
            )

            # Start x-coordinate from zero
            coords_i = coords_i - coords_i[0, 0]
            df_coords = coords_i

            # Store the data as 2D numpy array
            df_aux = df_channel[:, gage_cols].with_columns(
                pl.all().cast(pl.Float64, strict=False)
            )

            # Concatenate x-coords and data
            df_aux_2 = pl.concat([df_coords, df_aux])

            # Add to the dictionary with all the gages of this channel. We also
            # need to convert the data to float64.
            data_gages[gage] = df_aux_2
