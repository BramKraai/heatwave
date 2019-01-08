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

    SOURCE_NAME = "ghcnd_all"
    SOURCE_EXT = ".tar.gz"
    SOURCE_PATH = os.path.join(ROOT, f'{SOURCE_NAME}{SOURCE_EXT}')

    STATION_EXT = ".dly"

    NA = -9999

    def __init__(self, element: GHCNElement, country: Optional[Country]=None, span: Tuple[int, int]=(1979, 2017)):

        if not os.path.exists(GHCN.SOURCE_PATH):
            raise FileNotFoundError(f"Couldn't find {GHCN.SOURCE_PATH},\n"
                                    "Please make sure you've downloaded it from the GHCN website")

        if not os.path.exists(GHCN.INVENTORY_PATH):
            raise FileNotFoundError(f"Couldn't find {GHCN.INVENTORY_PATH},\n"
                                    "Please make sure you've downloaded it from the GHCN website")

        self._element = element.name
        self._country = country.name if country else ""
        self._span = span

        self._inventory = None

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

    @property
    def inventory(self):
        if self._inventory is None:
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

            inventory = inventory.set_index("ID")

            self._inventory = inventory

        return self._inventory

    def extract(self) -> None:
        # Create Set of ID's to Extract
        inventory_ids = set(self.inventory.index)
        station_data = OrderedDict()

        index = 0

        # Extract Data from Station Files
        with tarfile.open(GHCN.SOURCE_PATH) as source:

            # Loop through each Station File
            for station in source:

                # Parse Station ID from Station (File) Name
                station_id = station.name.replace(f"{self.SOURCE_NAME}/", "").replace(self.STATION_EXT, "")

                # If Station ID is in Inventory -> Extract
                if station_id in inventory_ids:
                    print(f"\rExtracting {index + 1:5d}/{len(inventory_ids):5d} : {station_id}", end="")

                    # Initialize Station Data Dictionary
                    station_data[station_id] = {}

                    # Loop Through Every Entry in Station File
                    for line in source.extractfile(station).read().decode().split('\n'):
                        if line:  # If Entry is Not Blank

                            # Parse Header and Data
                            header, data = line[:21], line[21:]
                            name, year, month, var = header[:11], int(header[11:15]), int(header[15:17]), header[17:21]

                            # If Entry is about target Variable and within target Year -> Extract
                            if self.span[0] <= year <= self.span[1] and self.element == var:
                                for day, value in enumerate([int(data[i:i+6]) for i in range(0, len(data), 8)], 1):
                                    station_data[station_id][f"{year:4d}-{month:02d}-{day:02d}"] = value

                    index += 1

        print()

        # Write CSV from Acquired Data
        with open(self.file, 'w') as output:
            station_ids = station_data.keys()
            output.write("DATE,{}\n".format(",".join(station_ids)))

            for day in self.daterange(date(self.span[0], 1, 1), date(self.span[1]+1, 1, 1)):
                day_string = day.strftime("%Y-%m-%d")
                values = [str(station_data[station_id].get(day_string, GHCN.NA)) for station_id in station_ids]
                output.write("{},{}\n".format(day_string, ", ".join(values)))

                print(f"\rWriting CSV -> {day_string}", end="")

        print()

    def load(self) -> pd.DataFrame:
        # Extract Data if not yet available
        if not os.path.exists(self.file):
            self.extract()

        # Return Dataframe
        return pd.read_csv(self.file)

    def daterange(self, start_date, end_date) -> Iterable[date]:
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)


if __name__ == '__main__':
    pd = GHCN(GHCNElement.TMAX, Country.US).load()

    print(pd)
