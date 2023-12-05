# Python reader for exported ODiSI data

## Description

This python package defines a reader and helper methods to handle data exported from the Luna ODiSI 6000 optical measuring system.
The benefit of using it is an easier retrieval of the data corresponding to each segment, as well as the possibility to interpolate the results based on additional measurements, such as experimental load.
Doing this manually requires some amount of python code, so I did it, so you don't have to.

## Installation

Install as usual:

```bash
pip install ...
```

## Usage

### Retrieve data from a \*.tsv file

The library can be used to read files in the following manner:

```python
from odisi import read_tsv

d = read_tsv("data_gages.tsv")

# List all gages
gages = d.gages
# List all segments
segments = d.segments
# Get the data for a specific gage, e.g. with the label 'A'
d_gage = d.gage("A")
# Get the data for a specific segment, e.g. with the label 'Seg-1'
d_seg = d.segment("Seg-1")
```

### Interpolation of data

The package allows to easily interpolate an external signal (e.g. the load during the test).
For this, two strategies can be followed:

#### 1. Interpolate the data from the sensors using the timestamps from the external signal

```python
import polars as pl

load = pl.read_csv("load_data.csv")
# Assume that the timestamp is in the column 'time'
d.interpolate(load.select(pl.col("time")))
```

Then you should be able to plot your data against the measured load:

```python
import matplotlib.pyplot as plt

d_gage = d.gage("A")
# Assume that the load data is in column 'load'
a_load = load.select(pl.col("load")).to_series()

plt.plot(d_gage, a_load)
```

#### 2. Interpolate the data from the external signal to match the timestamp from the sensor data

```python
import polars as pl

load = pl.read_csv("load_data.csv")
# Assume that the timestamp is in the column 'time'
new_load = d.interpolate_signal(data=load, time="time")
```

Then you should be able to plot your data against the measured load:

```python
import matplotlib.pyplot as plt

d_gage = d.gage("A")
# Assume that the load data is in column 'load'
a_load = new_load.select(pl.col("load")).to_series()

plt.plot(d_gage, a_load)
```

In both cases it is assumed that the timestamps from both files are synchronized, i.e. that both measuring computers have synchronized clocks.

### Clip data during interpolation

It is probable that the measurements from both data sources (ODiSI and additional system) were started at different times.
This produces some annoyances during the processing of the data due to the mismatch in datapoints.
To remedy this, the option `clip=True` can be passed to both interpolation methods (`interpolate(...)` and `interpolate_signal(...)`), which will clip the data to the common time interval between both signals.

```python
import polars as pl

load = pl.read_csv("load_data.csv")
# Assume that the timestamp is in the column 'time'
d.interpolate(load.select(pl.col("time")), clip=True)
```

## Tests

The package includes a test suite which should be run with pytest:

```bash
poetry run pytest
```

## Citation
