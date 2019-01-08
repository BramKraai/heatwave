from heatwave.enums import Country

import pandas as pd

from datetime import timedelta, date
from collections import OrderedDict
from typing import Tuple, Optional, Iterable
from enum import Enum, auto

import tarfile
import os


class GHCNElement(Enum):
    PRCP = auto()  # Precipitation(tenths of mm)
    SNOW = auto()  # Snowfall(mm)
    SNWD = auto()  # Snow depth(mm)
    TMAX = auto()  # Maximum temperature(tenths of degrees C)
    TMIN = auto()  # Minimum temperature(tenths of degrees C)


class GHCN:
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/GHCN'))

    INVENTORY_PATH = os.path.join(ROOT, 'ghcnd-inventory.txt')

    STATIONS_PATH = os.path.join(ROOT, 'ghcnd-stations.txt')

    SOURCE_NAME = "ghcnd_all"
    SOURCE_EXT = ".tar.gz"
    SOURCE_PATH = os.path.join(ROOT, f'{SOURCE_NAME}{SOURCE_EXT}')

    STATION_EXT = ".dly"

    NA = -9999

    def __init__(self, element: GHCNElement, country: Optional[Country]=None, span: Tuple[int, int]=(1979, 2017)):
        self._element = element.name
        self._country = country.name if country else ""
        self._span = span

    @property
    def file(self) -> str:
        return os.path.join(self.ROOT, f"ghcnd-{self.country}-{self.element}-{self.span[0]:4d}-{self.span[1]:4d}.csv")

    @property
    def element(self) -> str:
        return self._element

    @property
    def country(self) -> str:
        return self._country

    @property
    def span(self) -> Tuple[int, int]:
        return self._span

    def extract(self) -> None:
        # Load Inventory CSV as Pandas DataFrame
        inventory = pd.read_csv(
            GHCN.INVENTORY_PATH,
            sep="\s+",
            header=None,
            names=["ID", "LAT", "LON", "ELEMENT", "FIRSTYEAR", "LASTYEAR"],
            dtype={"ID": str, "LAT": float, "LON": float, "ELEMENT": str, "FIRSTYEAR": int, "LASTYEAR": int})

        # Add Country to DataFrame
        inventory["COUNTRY"] = [id[:2] for id in inventory["ID"]]

        # Filter Inventory
        inventory = inventory[inventory["COUNTRY"] == self.country]     # Filter by Country
        inventory = inventory[inventory["ELEMENT"] == self.element]     # Filter by Element
        inventory = inventory[inventory["FIRSTYEAR"] <= self.span[0]]   # Filter by First Year
        inventory = inventory[inventory["LASTYEAR"] >= self.span[1]]    # Filter by Last Year

        # Create Set of ID's to Extract
        inventory_ids = set(inventory["ID"])

        index = 0

        station_data = OrderedDict()

        # Extract Data from Station Files
        with tarfile.open(GHCN.SOURCE_PATH) as source:

            for station in source:
                station_id = station.name.replace(f"{self.SOURCE_NAME}/", "").replace(self.STATION_EXT, "")

                if station_id in inventory_ids:
                    print(f"\rExtracting {index + 1:5d}/{len(inventory_ids):5d} : {station_id}", end="")

                    station_data[station_id] = {}

                    for line in source.extractfile(station).read().decode().split('\n'):
                        if line:
                            header, data = line[:21], line[21:]

                            name, year, month, var = header[:11], int(header[11:15]), int(header[15:17]), header[17:21]

                            if self.span[0] <= year <= self.span[1] and self.element == var:
                                for day, value in enumerate([int(data[i*8:i*8+6]) for i in range(31)]):
                                    station_data[station_id][f"{year:4d}-{month:02d}-{day:02d}"] = value

                    index += 1

        print()

        # Writing CSV
        with open(self.file, 'w') as output:
            station_ids = station_data.keys()
            output.write("DATE, {}\n".format(", ".join(station_ids)))

            for day in self.daterange(date(self.span[0], 1, 1), date(self.span[1]+1, 1, 1)):
                day_string = day.strftime("%Y-%m-%d")
                values = [str(station_data[station_id].get(day_string, GHCN.NA)) for station_id in station_ids]
                output.write("{}, {}\n".format(day_string, ", ".join(values)))

                print(f"\rWriting {day_string}", end="")

        print()

    def load(self) -> pd.DataFrame:
        # Extract Data if not yet available
        if not os.path.exists(self.file):
            self.extract()

        # Return Dataframe
        return pd.DataFrame(self.file)

    def daterange(self, start_date, end_date) -> Iterable[date]:
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)


if __name__ == '__main__':
    ghcn = GHCN(GHCNElement.TMAX, Country.US)
    ghcn.extract()
