from heatwave.enums import Country

import numpy as np
import netCDF4

from matplotlib import pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

from shapely import geometry
import shapefile

import os


DATA_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data'))


def plot_earth(view="EARTH"):
    # Create Big Figure
    plt.rcParams['figure.figsize'] = [25, 10]

    # create Projection and Map Elements
    projection = ccrs.PlateCarree()
    ax = plt.axes(projection=projection)
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS)
    ax.add_feature(cfeature.STATES)
    ax.add_feature(cfeature.OCEAN, color="white")
    ax.add_feature(cfeature.LAND, color="lightgray")

    if view == "US":
        ax.set_xlim(-130, -65)
        ax.set_ylim(24, 50)
    elif view == "EAST US":
        ax.set_xlim(-105, -65)
        ax.set_ylim(25, 50)
    elif view == "EARTH":
        ax.set_xlim(-180, 180)
        ax.set_ylim(-90, 90)

    return projection


def country_mask(coordinates):
    shapefile_path = os.path.join(DATA_ROOT, 'misc/ne_50m_admin_0_countries/ne_50m_admin_0_countries.shp')

    countries = shapefile.Reader(shapefile_path)

    iso_a2_index = [field[0] for field in countries.fields[1:]].index("ISO_A2")

    shapes = [geometry.shape(shape) for shape in countries.shapes()]
    records = [record[iso_a2_index] for record in countries.records()]

    country_codes = np.empty(len(coordinates), np.int)

    for index, coordinate in enumerate(coordinates):
        if len(coordinates) > 1000:
            print(f"\rCreating Country Mask: Grid Cell "
                  f"{index+1:7d}/{len(coordinates):7d} "
                  f"({float(index)/float(len(coordinates)):3.1%})", end="")

        point = geometry.Point(coordinate)

        for shape, iso_a2 in zip(shapes, records):
            if point.within(shape):
                country_codes[index] = Country[iso_a2] if iso_a2 in Country.__members__ else -1
                break
            else:
                country_codes[index] = -1

    if len(coordinates) > 1000:
        print()

    return country_codes


def era_coordinate_grid(path):
    # Get Latitudes and Longitudes from ERA .nc file
    era = netCDF4.Dataset(path)

    if 'latitude' in era.variables and 'longitude' in era.variables:
        latitudes = era['latitude'][:]
        longitudes = era['longitude'][:]
    elif 'lat' in era.variables and 'lon' in era.variables:
        latitudes = era['lat'][:]
        longitudes = era['lon'][:]
    else:
        raise AttributeError("path contains neither 'latitude'/'longitude' nor 'lat'/'lon' fields")

    # Create Coordinate Grid
    coordinates = np.empty((len(latitudes), len(longitudes), 2), np.float32)
    coordinates[..., 0] = longitudes.reshape(1, -1)
    coordinates[..., 1] = latitudes.reshape(-1, 1)

    return coordinates


def era_country_mask(path):
    mask_file = os.path.splitext(path)[0] + '_mask.npy'

    if os.path.exists(mask_file):
        return np.load(mask_file)

    # Load Coordinates and Normalize to ShapeFile Coordinates
    coordinates = era_coordinate_grid(path)
    coordinates[..., 0][coordinates[..., 0] > 180] -= 360

    # Take Center of Grid Cell as Coordinate
    coordinates[..., 0] += (coordinates[0, 1, 0] - coordinates[0, 0, 0]) / 2
    coordinates[..., 1] += (coordinates[1, 0, 1] - coordinates[0, 0, 1]) / 2

    # Create Mask
    mask = country_mask(coordinates.reshape(-1, 2)).reshape(coordinates.shape[:2])

    np.save(mask_file, mask)

    return mask
