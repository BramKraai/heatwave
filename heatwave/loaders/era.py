import netCDF4
import numpy as np
import pandas as pd


class ERA:
    LONGITUDE = 'longitude'
    LATITUDE = 'latitude'

    TIME = 'time'
    TIME_UNIT = 'h'
    TIME_ORIGIN = "1900-01-01"

    def __init__(self, path, target):
        self._dataset = netCDF4.Dataset(path)
        self._target = target

        self._latitude = self._dataset[ERA.LATITUDE][:].data
        self._longitude = self._dataset[ERA.LONGITUDE][:].data

        self._time = self._dataset[ERA.TIME][:]
        self._time = pd.DataFrame(data=np.arange(len(self._time)),
                                  index=pd.to_datetime(self._time, unit=ERA.TIME_UNIT, origin=ERA.TIME_ORIGIN))

    @property
    def data(self) -> np.ndarray:
        return self._dataset[self._target][:]

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

    def anomaly(self):
        data = self.data
        df = pd.DataFrame(data.reshape(len(self.time), -1), index=self.time.index)
        df = df.groupby([df.index.month, df.index.day]).apply(lambda x: x - x.mean())
        data[:] = df.values.reshape(data.shape)
        return data
