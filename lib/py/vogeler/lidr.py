#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Daniel Rode


"""Functions that mainly depend on the lidR R library and rpy2."""


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Import libraries
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# Import standard libraries
from pathlib import Path
from logging import ERROR

# Import external libraries
import shapely

# Import R libraries
from rpy2.robjects.packages import importr
from rpy2.rinterface_lib.callbacks import logger as rpy2_logger
from rpy2.rinterface import NULL as R_NULL
grDevices = importr("grDevices")
lidr = importr("lidR")
rpy2_logger.setLevel(ERROR)  # Suppress R warning messages
grDevices.pdf(R_NULL)  # Prevent creation of Rplots.pdf


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def clip_las(
    src_las_path: Path,
    bounds: shapely.Polygon,
    dst_las_path: Path,
) -> None:
    """TODO"""

    # Remove Z dimension from polygon (if it has one)
    bounds = shapely.force_2d(bounds)

    # Clip catalog to given polygon and save as new point cloud file
    ctg = lidr.readLAScatalog(str(src_las_path))
    las = lidr.clip_roi(ctg, bounds.wkt)
    lidr.writeLAS(las, str(dst_las_path))
