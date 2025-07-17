#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Daniel Rode


"""Functions that depend on third-party Python libraries."""


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Import libraries
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# Import standard libraries
import os
import functools
from pathlib import Path
from logging import ERROR
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed

from typing import Any
from collections.abc import Callable
from collections.abc import Iterator

# Import in-house libraries
from vogeler.sp import build_vpc

# Import external libraries
import dill
import pyogrio
import shapely
import numpy as np
import pandas as pd
import rasterio as rio
from rasterio.mask import mask
from rasterio.fill import fillnodata
from geopandas import GeoDataFrame
from pathos.pools import ProcessPool

# Import R libraries
from rpy2.robjects.packages import importr
from rpy2.rinterface_lib.callbacks import logger as rpy2_logger
rbase = importr("base")
rmethods = importr("methods")
rpy2_logger.setLevel(ERROR)  # Suppress R warning messages


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

MAX_WORKERS = os.cpu_count()


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def papply(jobs: iter, worker: Callable, max_workers=MAX_WORKERS) -> list:
    """
    Parallel apply dispatching function: Run a list of jobs with a given
    worker function, in parallel, and return the results in order of jobs
    list. Uses pathos as backend.
    """
    # Run workers in parallel
    with ProcessPool(ncpus=max_workers) as executor:
        try:
            results = executor.map(worker, jobs)
        except Exception as e:
            # If Python is sent a kill signal, kill all the workers too
            executor.terminate()
            raise e

    return results


def run_dill(fn: Callable, *args, **kwargs) -> Any:
    """Helper function to use dill instead of pickle.

    This allows multiprocessing to accept lambda functions.
    """

    return dill.loads(fn)(*args, **kwargs)


def dispatch(
    jobs: iter, worker: Callable, max_workers=MAX_WORKERS,
) -> Iterator[(Any, Any)]:
    """Run jobs in parallel.

    Parallel apply dispatching function: Run a list of jobs with a given
    worker function, in parallel, and yield worker results in order they
    finish.
    """

    worker = functools.partial(run_dill, dill.dumps(worker))  # Allow lambdas
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures_jobs = {executor.submit(worker, j): j for j in jobs}
        for f in as_completed(futures_jobs):
            log.info("Worker finished: %s", futures_jobs[f])
            yield (futures_jobs[f], f.result())


def rds2vpc(rds_path: Path, out_path: Path) -> None:
    """
    Extract list of source LAS/LAZ file paths from lidR las catalog object
    that was saved as an RDS file and build a virtual point cloud (VPC)
    catalog with that file set.
    """

    # Get set of LiDAR collection LAS/LAZ paths
    ctg = rbase.readRDS(rds_path)
    paths = rmethods.slot(ctg, 'data').rx2('filename')  # R: ctg@data$filename
    paths = set(paths)

    # Build VPC
    build_vpc(paths, out_path)


def wkt2gdf(wkt: str, crs: str) -> GeoDataFrame:
    """Create a GeoDataFrame from a WKT polygon string."""

    polygon = shapely.from_wkt(wkt)
    gdf = GeoDataFrame(geometry=[polygon], crs=crs)

    return gdf


def catgdf(gdf_list: list[GeoDataFrame]) -> GeoDataFrame:
    """Vertically concatenate list of GeoDataFrames into one GeoDataFrame."""

    # Drop empty dataframes
    gdf_list = [
        gdf for gdf in gdf_list if len(gdf) != 0
    ]

    # Handle if gdf_list is empty
    if not gdf_list:
        return GeoDataFrame()

    # Verify all CRSs match
    assert len({g.crs for g in gdf_list}) == 1

    # Concatenate GeoDataFrames
    gdf = GeoDataFrame(
        pd.concat(gdf_list, axis=0, ignore_index=True),
        crs=gdf_list[0].crs,
    )

    return gdf


def catshp(shp_list: list[Path], dst_path: Path) -> None:
    """Vertically concatenate list of shapefiles; save as single shapefile."""

    # Load shapefiles
    gdf_list = [
        pyogrio.read_dataframe(shp) for shp in shp_list
    ]

    # Drop empty dataframes
    gdf_list = [
        gdf for gdf in gdf_list if len(gdf) != 0
    ]

    # Concatenate shapefiles and save to new file
    gdf = catgdf(gdf_list)
    gdf.to_file(dst_path)


def fill_grid_na(
    arr: np.ndarray, neighborhood=128, fill_all=True,
) -> np.ndarray:
    """Fill-in raster NA values using nearest neighbor.

    Interpolate the nan values of a 2D or 3D Numpy array using nearest
    neighbor values. If "fill_all" is false, this function will only fill
    values within the "neighborhood" distance away from the nearest non-nan
    value; if "fill_all" is true, filling is done iteratively until all indexes
    have a non-nan value.
    """

    arr = np.copy(arr)
    while np.isnan(arr).sum() != 0:
        arr = fillnodata(
            arr, ~np.isnan(arr), max_search_distance=neighborhood,
        )
        if not fill_all:
            return arr
    return arr


def tif_m2ft(src_tif: Path, dst_tif: Path) -> None:
    """Convert rasters meters to feet.

    Convert the pixel values of a given raster from meters to feet and save
    as new raster.

    1 ft = 12 in
    100 cm = 1 m
    1 in = 2.54 cm
    1 in * 12 = 2.54 cm * 12
    12 in = 2.54 cm * 12
    1 ft = 2.54 cm * 12
    1 ft = 30.48 cm
    1 ft / 30.48 = 30.48 cm / 30.48
    1 ft / 30.48 = 1 cm
    1 ft / 30.48 * 100 = 1 cm * 100
    1 ft / 30.48 * 100 = 100 cm
    1 ft / 30.48 * 100 = 1 m
    100 ft / 30.48 = 1 m
    1 m = (100 / 30.48) ft
    """

    # Import source raster (the one with pixel values in meters)
    with rio.open(src_tif) as f:
        # Read the metadata
        profile = f.profile.copy()
        pixels = f.read()

    # Convert pixel values from meters to feet
    pixels_ft = pixels * (100 / 30.48)

    # Verify pixels' data type remained the same after value conversion
    assert pixels_ft.dtype == profile['dtype']

    # Write new raster to file
    with rio.open(dst_tif, 'w', **profile) as f:
        f.write(pixels_ft)

def update_crs_metadata(tif_path: Path, epsg: int) -> None:
    """Update a given raster's CRS metadata."""

    with rio.open(tif_path, 'r+') as f:
        f.crs = rio.crs.CRS.from_epsg(epsg).wkt


def clip_tif(tif_path: Path, shp_path: Path, out_path: Path) -> None:
    """Clip a raster file to a given shape boundary file."""

    # Import polygon data
    gdf = pyogrio.read_dataframe(shp_path)

    # Import region of raster data within polygons
    with rio.open(tif_path) as f:
        if not gdf.crs:
            gdf = GeoDataFrame(geometry=gdf['geometry'], crs=f.crs)
        rast, rast_transform = mask(
            f, gdf.to_crs(f.crs)['geometry'], crop=True, all_touched=True,
        )
        rast_meta = f.meta

    # Save clipped raster to file
    _, rast_meta["height"], rast_meta["width"] = rast.shape
    rast_meta["driver"] = "GTiff"
    rast_meta["transform"] = rast_transform
    with rio.open(out_path, "w", **rast_meta) as f:
        f.write(rast)
