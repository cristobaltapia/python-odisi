from datetime import datetime, timedelta

import numpy as np
import polars as pl
from numpy.typing import NDArray

from odisi.utils import ar_timedelta, timedelta_sec


class OdisiResult:

    """Contains the data from the experiment.

    Attributes
    ----------
    data : obj:`DataFrame`
        A dataframe with the data of the experiment.
    x : ArrayLike
        The measurement positions along the sensor.
    gages : list[str]
        A list containing the name of each gage.
    segments : list[str]
        A list containing the name of each segment.
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
    ) -> pl.DataFrame:
        """Interpolate the sensor data to match the timestamp of the given array.

        This method assumes that the timestamp in `time` is synchronized with the
        timestamp of the measured data, i.e. both measuring computers have the
        same time.

        Parameters
        ----------
        time : NDArray[datetime64]
            Array with the time used to interpolate the sensor data.
        clip : bool (False)
            Whether the interpolated data should only consider timestamps common
            to both `time` and senor data.
        relative_time : bool (False)
            Signals whether the values in `time` correspond to relative delta
            times in seconds. These data will then be converted to `Datetime`
            objects in order to perform the interpolation.

        Returns
        -------
        time : pl.DataFrame
            The interpolated timestamp as dataframe.

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

        # Clip the data if requested
        if clip:
            # Get max/min timestamp for both Dataframes
            min_t = time.select(pl.col("time")).min()[0, 0]
            max_t = time.select(pl.col("time")).max()[0, 0]
            min_d = data.select(pl.col("time")).min()[0, 0]
            max_d = data.select(pl.col("time")).max()[0, 0]
            clip_low = max(min_t, min_d)
            clip_up = min(max_t, max_d)
            # Filter the data
            time = time.filter((pl.col("time") >= clip_low) & (pl.col("time") <= clip_up))
            data = data.filter((pl.col("time") >= clip_low) & (pl.col("time") <= clip_up))

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

        return time

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
            to be interpolated. If given, then column name for the time should be given
            in the parameters `time`.
        time : str | NDArray (None)
            If `data` is given, then this parameters takes the name of the column containing the timestamp to be considered for the interpolation. Otherwise,
            this should be an array with the timestamp for the interpolation.
        signal : str | NDArray (None)
            If `data` is given, then this parameters is not needed. Otherwise, this
            should be an array with the signal to be interpolated.
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
