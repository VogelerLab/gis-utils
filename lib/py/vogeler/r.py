#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Daniel Rode


"""Functions that allow Python to interface with R packages."""


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Import libraries
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# Import standard libraries
from logging import ERROR

# Import R libraries
from rpy2.robjects import r
from rpy2.robjects.methods import RS4
from rpy2.rinterface import NULL as R_NULL
from rpy2.robjects.vectors import IntVector
from rpy2.robjects.packages import importr as rlib
from rpy2.rinterface_lib.callbacks import logger as rpy2_logger

rpy2_logger.setLevel(ERROR)  # Suppress R warning messages

sf = rlib("sf")


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def get_ctg_epsgs(ctg: RS4) -> list[int]:
    """Given a lidR LAS Catalog object, return its EPSGs as list."""

    ctg_data = ctg.do_slot("data")
    epsg_list = ctg_data[ctg_data.names.index("CRS")]

    return list(epsg_list)


def get_ctg_geoms_wkt(ctg: RS4) -> list[str]:
    """Convert lidR LAS catalog geometries as WKT.

    Given a lidR LAS Catalog object, return its geometries as a list of
    wkt strings.
    """

    # R: ctg@data$geometry
    ctg_data = ctg.do_slot("data")
    geoms = ctg_data[ctg_data.names.index('geometry')]

    geoms_wkt = sf.st_as_text(geoms)

    return list(geoms_wkt)


def get_las_crs_wkt(las: RS4) -> str:
    """Given a lidR LAS object, return its CRS as a WKT string."""

    sf_crs_obj = sf.st_crs(las.slots['crs'])
    wkt = sf_crs_obj[sf_crs_obj.names.index('wkt')][0]

    return wkt
