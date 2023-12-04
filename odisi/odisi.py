from datetime import datetime, timedelta

import numpy as np
import polars as pl
from numpy.typing import ArrayLike, NDArray

from odisi.utils import ar_timedelta, timedelta_sec


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
        self._gages: dict[str, int] = {}
        self._segments: dict[str, list[int]] = {}
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

    @property
    def gages(self) -> list[str]:
        return list(self._gages.keys())

    @property
    def segments(self) -> list[str]:
        return list(self._segments.keys())

    def gage(self, label: str, with_time: bool = False) -> pl.DataFrame:
        """Get data corresponding to the given gauge.

        Parameters
        ----------
        label : str
            The label of the gage.
        with_time : bool
            Whether a column with the time should also be returned in the dataframe.

        Returns
        -------
        df : pl.DataFrame
            Dataframe with the data corresponding to the gage.

        """
        # Check that the label exists
        if label not in self.gages:
            raise KeyError("The given gage label does not exist.")

        ix_gage = self.data.columns[self._gages[label]]
        if with_time:
            return self.data.select(pl.col(["time", ix_gage]))
        else:
            return self.data.select(pl.col(ix_gage))

    def segment(self, label: str, with_time: bool = False) -> pl.DataFrame:
        """Get data corresponding to the given segment.

        Parameters
        ----------
        label : str
            Tha label of the segment.
        with_time : bool
            Whether a column with the time should also be returned in the dataframe.

        Returns
        -------
        df : pl.DataFrame
            Dataframe with the data corresponding to the segment.

        """
        # Check that the label exists
        if label not in self.segments:
            raise KeyError("The given segment label does not exist.")

        # Get start and end indices delimiting the column range for the segment
        s, e = self._segments[label]
        # Get the column name of the corresponding columns
        ix_segment = self.data.columns[s : e + 1]

        if with_time:
            return self.data.select(pl.col(["time", *ix_segment]))
        else:
            return self.data.select(pl.col(ix_segment))

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

    def interpolate(
        self,
        time: NDArray[np.datetime64] | pl.DataFrame,
        clip: bool = False,
        relative_time: bool = False,
    ) -> None:
        """Interpolate the sensor data to match the timestamp of the given array.

        This method assumes that the timestamp in `time` is synchronized with the
        timestamp of the measured data.

        Parameters
        ----------
        time : NDArray[datetime64]
            Array with the time used to interpolate the sensor data.
        keep_sample_rate : bool
            Whether the sample rate of the original sensor data should be preserved.
        relative_time : bool (False)
            Singnals whether the values in `time` correspond to relative delta
            times in seconds. These data will then be converted to `Datetime`
            objects in order to perform the interpolation.

        Returns
        -------
        None

        """
        data = self.data

        # Ensure the correct name for the column
        if isinstance(time, pl.DataFrame):
            time = time.rename({time.columns[0]: "time"})
        # Convert time to polars DataFrame if needed
        else:
            time = pl.DataFrame({"time": time})

        # Consider relative time data
        if relative_time:
            # Get initial timestamp from sensor data
            t_init = data[0, 0]
            time = time.with_columns(
                pl.col("time").map_elements(timedelta_sec).add(t_init)
            )

        # Do the interpolation
        aux, _ = pl.align_frames(data, time, on="time")

        # Interpolate data
        df_sync = aux.interpolate()

        # Now get only the data associated to the load data
        ix_load = [k[0] in time[:, 0] for k in df_sync.select("time").iter_rows()]
        df_sync = df_sync.filter(ix_load)

        # Update rate
        self._rate = (df_sync[1, 0] - df_sync[0, 0]).total_seconds()
        # Update data
        self.data = df_sync

        return None

    def interpolate_signal(
        self,
        data: pl.DataFrame | None = None,
        time: str | NDArray | None = None,
        signal: str | NDArray | None = None,
        relative_time: bool = False,
        clip: bool = False,
    ) -> pl.DataFrame:
        """Interpolate an external signal, such that it matches the data from the sensor.

        Parameters
        ----------
        data : pl.Dataframe (None)
            Dataframe containing a column for the timestamp and another for the signal
            to be interopolated. If given, then column name for the time and signal
            should be given in the parameters `time` and `signal` respectively.
        time : str | NDArray (None)
            If `data` is given, then this parameters takes the name of the column containing the timestamp to be considered for the interpolation. Otherwise,
            this should be an array with the timestamp for the interpolation.
        signal : str | NDArray (None)
            If `data` is given, then this parameters takes the name of the column
            containing the data to be interpolated. Otherwise, this should be an
            array with the signal to be interpolated.
        clip : TODO, optional

        Returns
        -------
        TODO

        """
        data_sensor = self.data

        # Ensure the correct name for the column
        if isinstance(data, pl.DataFrame):
            data = data.rename({time: "time"})
        # Convert time to polars DataFrame if needed
        else:
            data = pl.DataFrame({"time": time, "signal": signal})

        # Do the interpolation
        _, aux = pl.align_frames(data_sensor, data, on="time")

        # Interpolate data
        df_sync = aux.interpolate()

        sensor_time = data_sensor.select(pl.col("time"))
        # Now get only the data associated to the load data
        ix_load = [
            k[0] in sensor_time[:, 0] for k in df_sync.select("time").iter_rows()
        ]
        df_sync = df_sync.filter(ix_load)

        return df_sync


class OdisiGagesResult(OdisiResult):

    """Docstring ."""

    def __init__(
        self,
        data,
        x,
        gages: dict[str, int],
        segments: dict[str, list[int]],
        metadata,
    ):
        """TODO: to be defined.

        Parameters
        ----------
        segments : TODO


        """
        super().__init__(data, x, metadata)
        self._gages = gages
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
