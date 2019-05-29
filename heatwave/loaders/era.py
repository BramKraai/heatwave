import netCDF4

import numpy as np
import pandas as pd

from typing import List, Tuple


class ERA:
    def __init__(self, path: str, target: str, index: List[slice] = (),
                 latitude_key: str = 'latitude', longitude_key: str = 'longitude',
                 time_key: str = 'time', time_unit: str = 'h', time_origin: str = '1900-01-01'):

        self._path = path

        self._latitude_key = latitude_key
        self._longitude_key = longitude_key

        self._time_key = time_key
        self._time_unit = time_unit
        self._time_origin = time_origin

        self._dataset = netCDF4.Dataset(path)
        self._target = target
        self._index = [
            index[0] if len(index) >= 1 and index[0] is not None else slice(0, len(self._dataset[self._time_key])),
            index[1] if len(index) >= 2 and index[1] is not None else slice(0, len(self._dataset[self._latitude_key])),
            index[2] if len(index) >= 3 and index[2] is not None else slice(0, len(self._dataset[self._longitude_key]))
        ]

        self._time = self._dataset[self._time_key]
        self._time = netCDF4.num2date(self._time[self._index[0]], units=self._time.units, calendar=self._time.calendar)
        self._time = [f"{date.year:04d}-{date.month:02d}-{date.day:02d}" for date in self._time]
        self._time = pd.DataFrame(data=np.arange(len(self._time)), index=pd.to_datetime(self._time))

        self._latitude = self._dataset[self._latitude_key][self._index[1]].data
        self._longitude = self._dataset[self._longitude_key][self._index[2]].data

        self._shape = len(self._time), len(self._latitude), len(self.longitude)

        self._data = None
        self._anomaly = None

    @property
    def path(self):
        return self._path

    @property
    def shape(self) -> Tuple[int, int, int]:
        return self._shape

    @property
    def data(self) -> np.ndarray:
        if self._data is None:
            self._data = self._dataset[self._target][self._index]
            self._data[self._data.mask] = np.nan
            self._data = np.array(self._data)
        return self._data

    @property
    def dataset(self) -> netCDF4.Dataset:
        return self._dataset

    @property
    def latitude(self) -> np.ndarray:
        return self._latitude

    @property
    def longitude(self) -> np.ndarray:
        return self._longitude

    @property
    def time(self) -> pd.DataFrame:
        return self._time

    @property
    def anomaly(self) -> np.ndarray:
        if self._anomaly is None:
            self._anomaly = self.data.copy()
            df = pd.DataFrame(self._anomaly.reshape(len(self.time), -1), index=self.time.index)
            df = df.groupby([df.index.month, df.index.day]).apply(lambda x: x - x.mean())
            self._anomaly[:] = df.values.reshape(self._anomaly.shape)
        return self._anomaly

    def reindex(self, index: List[slice] = ()):
        new_index = []
        for old, new in zip(self._index, index):
            if isinstance(old, slice) and isinstance(new, slice):
                new_index.append(slice(old.start + new.start, old.start + new.stop))
            else:
                new_index.append(new)

        return ERA(self._path, self._target, new_index,
                   self._latitude_key, self._longitude_key,
                   self._time_key, self._time_unit, self._time_origin)

    def __repr__(self):
        return f"ERA({self._target}) {self.shape}"
