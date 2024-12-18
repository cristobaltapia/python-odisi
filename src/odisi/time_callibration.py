import sys
from datetime import timedelta

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axes import Axes
from numpy.typing import NDArray

import odisi


class VisualTimeFitter:
    """
    Class used to manually determine the time shift between both signals.

    Parameters
    ----------
    line :
        Matplotlib line object.
    ax :
        Matplotlib axis containing the line.
    delta_t : timedelta
        Initial timedelta to be applied to the data.

    """

    def __init__(self, line, ax: Axes, delta_t: timedelta):
        self.line = line
        self.xs = list(line.get_xdata())
        self.ys = list(line.get_ydata())
        self.cid = line.figure.canvas.mpl_connect("button_press_event", self)
        self.first_click = True
        self.prev_xdata = None
        self._delta_t: np.timedelta64 = timedelta2numpy(delta_t)
        # Add timedelta to the sensor datetime information
        self.ax = ax
        self.state_shift = False
        self.mark_1 = ax.axvline(self.xs[0], color="k", alpha=0)
        # Add current deltatime
        self.current_dt = ax.annotate(
            f"Δt: {self.delta_t}",
            xy=(0.98, 0.02),
            xycoords="axes fraction",
            va="bottom",
            ha="right",
        )
        # Redraw line to consider the initial timedelta
        self.xs += self._delta_t
        self.line.set_data(self.xs, self.ys)
        self.line.figure.canvas.draw()

    @property
    def delta_t(self) -> timedelta:
        return self._delta_t.tolist()

    def connect(self):
        """Connect to all the events we need."""
        self.cidpress = self.line.figure.canvas.mpl_connect(
            "button_press_event", self.on_press
        )
        self.cidrelease = self.line.figure.canvas.mpl_connect(
            "button_release_event", self.on_release
        )
        self.cidkeypress = self.line.figure.canvas.mpl_connect(
            "key_press_event", self.on_keypress
        )
        self.cidkeyrelease = self.line.figure.canvas.mpl_connect(
            "key_release_event", self.on_keyrelease
        )

    def on_keypress(self, event):
        if event.key == "shift":
            self.state_shift = True

    def on_keyrelease(self, event):
        if event.key == "shift":
            self.state_shift = False

    def on_release(self, event):
        return None

    def on_press(self, event):
        # Ignore this method if the key 'shift' is pressed
        if self.state_shift:
            return

        if self.first_click:
            # Get date at clicked location
            self.prev_xdata = mdates.num2date(event.xdata)
            # Mark begin of callibration cycle
            self.first_click = False
            # Update plot
            self.mark_1.set_data([event.xdata, event.xdata], [0, 1])
            self.mark_1.set_alpha(1)
            self.line.figure.canvas.draw()
            return None
        else:
            first_x = self.prev_xdata
            second_x = mdates.num2date(event.xdata)
            delta = timedelta2numpy(second_x - first_x)
            # Add timedelta to the sensor datetime information
            self.xs += delta
            # Update timedelta used for the sensor
            self._delta_t += delta
            # Update annotation text
            self.current_dt.set_text(f"Δt: {self.delta_t}")
            # Mark end of callibration cycle
            self.first_click = True
            # Update plot
            new_x = mdates.date2num(second_x)
            self.mark_1.set_data([new_x, new_x], [0, 1])
            self.mark_1.set_alpha(1)
            self.line.set_data(self.xs, self.ys)
            self.line.figure.canvas.draw()

    def __call__(self, event):
        return None


def timedelta2numpy(delta: timedelta) -> np.timedelta64:
    """Transform from datetime.timedelta to numpy.timedelta64.

    Parameters
    ----------
    delta : TODO

    Returns
    -------
    np.timedelta64

    """
    delta_seconds = delta.total_seconds()
    delta_microseconds = delta.microseconds
    # Get seconds as int
    delta_s = int(np.floor(delta_seconds))
    # Get milliseconds
    delta_ms = int(delta_microseconds / 1000)
    # Create a numpy timedelta object
    delta = np.timedelta64(delta_s, "s") + np.timedelta64(delta_ms, "ms")

    return delta


def calibrate_timedelta(
    luna_data: NDArray,
    luna_time: NDArray,
    ext_data: NDArray,
    ext_time: NDArray,
    init_delta: timedelta,
) -> timedelta:
    """Manually determine the time shift between both data.

    A graphical interface is used to determine the time shift needed.

    Instructions for the user intertface:
        - First click will mark an arbitrary date as reference
        - Second click will mark the date to which the first point should be
          translated. The difference between these values determines the
          timedelta. This process can be repeated until the window is closed,
          after which the determined timedelta is returned.
        - Holding the key `shift` suppress the above behavior. Thus, this key
          can be pressed when other actions are needed in the plot (e.g. move
          or zoom).

    Parameters
    ----------
    luna_data : NDArray
        Data corresponding to one point along the glass fiber.
    luna_time : NDArray
        Datetime information corresponding to the LUNA System.
    ext_data : NDArray
        External data that needs to be synchronized.
    ext_time : NDArray
        Datetime information corresponding to the external data.
    init_delta : timedelta
        Initial time shift.

    Returns
    -------
    dt : timedelta

    """
    # Create matplotlib figure
    fig = plt.figure()
    # Creat axes sharing their x-axis
    ax_luna = fig.add_subplot(111)
    ax_load = ax_luna.twinx()

    # Plot the external data
    ax_luna.plot(ext_time, ext_data, color="C0", label="External data")
    # Plot the data from the LUNA system and store the line object
    (line,) = ax_load.plot(luna_time, luna_data, color="C3", label="LUNA data")
    # Instantiate the visual time fitter
    time_fit = VisualTimeFitter(line, ax_load, delta_t=init_delta)
    time_fit.connect()

    ax_luna.legend(loc="upper right")
    ax_load.legend(loc="upper left")

    # Only show the interactive plot if we are not in a pytest session
    if not odisi._called_from_test:
        plt.show()
        plt.close()

    return time_fit.delta_t
