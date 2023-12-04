from datetime import timedelta
import numpy as np


def timedelta_sec(s):
    return timedelta(seconds=s)


ar_timedelta = np.vectorize(timedelta_sec)
