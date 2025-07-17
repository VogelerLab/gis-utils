#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Daniel Rode


"""Functions that only depend on the Python standard library."""


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Import libraries
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# Import standard libraries
import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed as cf_as_completed

from typing import Any
from collections.abc import Callable
from collections.abc import Iterator


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

MET20M_DIR = 'SD_BlackHills_D23/GridMetrics/Metrics_20Meters/'
STRAT_DIR = 'SD_BlackHills_D23/GridMetrics/StrataMetrics_20Meters/'
NAMING_MAP = {
    'gridmet20m_all_count.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Count_20m.tif',
    'gridmet20m_all_kde_peak1_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peak1_Diff_20m.tif',
    'gridmet20m_all_kde_peak1_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peak1_Elev_20m.tif',
    'gridmet20m_all_kde_peak1_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peak1_Value_20m.tif',
    'gridmet20m_all_kde_peak2_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peak2_Diff_20m.tif',
    'gridmet20m_all_kde_peak2_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peak2_Elev_20m.tif',
    'gridmet20m_all_kde_peak2_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peak2_Value_20m.tif',
    'gridmet20m_all_kde_peak3_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peak3_Diff_20m.tif',
    'gridmet20m_all_kde_peak3_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peak3_Elev_20m.tif',
    'gridmet20m_all_kde_peak3_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peak3_Value_20m.tif',
    'gridmet20m_all_kde_peak4_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peak4_Elev_20m.tif',
    'gridmet20m_all_kde_peak4_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peak4_Value_20m.tif',
    'gridmet20m_all_kde_peaks_count.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_KDE_Peaks_Count_20m.tif',
    'gridmet20m_all_L1_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Lmoment_1_20m.tif',
    'gridmet20m_all_L2_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Lmoment_2_20m.tif',
    'gridmet20m_all_L3_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Lmoment_3_20m.tif',
    'gridmet20m_all_L4_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Lmoment_4_20m.tif',
    'gridmet20m_all_Lcoefvar.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Lmoment_CV_20m.tif',
    'gridmet20m_all_Lkurt.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Lmoment_Kurt_20m.tif',
    'gridmet20m_all_Lskew.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Lmoment_Skew_20m.tif',
    'gridmet20m_all_rumple.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Rumple_20m.tif',
    'gridmet20m_all_z_cv.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_CV_20m.tif',
    'gridmet20m_all_z_kurt.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_Kurt_20m.tif',
    'gridmet20m_all_z_max_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_Max_20m.tif',
    'gridmet20m_all_z_mean_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_Mean_20m.tif',
    'gridmet20m_all_z_min_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_Min_20m.tif',
    'gridmet20m_all_z_p01_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P01_20m.tif',
    'gridmet20m_all_z_p02_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P02_20m.tif',
    'gridmet20m_all_z_p05_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P05_20m.tif',
    'gridmet20m_all_z_p10_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P10_20m.tif',
    'gridmet20m_all_z_p20_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P20_20m.tif',
    'gridmet20m_all_z_p25_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P25_20m.tif',
    'gridmet20m_all_z_p30_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P30_20m.tif',
    'gridmet20m_all_z_p40_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P40_20m.tif',
    'gridmet20m_all_z_p50_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P50_20m.tif',
    'gridmet20m_all_z_p60_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P60_20m.tif',
    'gridmet20m_all_z_p70_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P70_20m.tif',
    'gridmet20m_all_z_p75_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P75_20m.tif',
    'gridmet20m_all_z_p80_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P80_20m.tif',
    'gridmet20m_all_z_p90_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P90_20m.tif',
    'gridmet20m_all_z_p95_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P95_20m.tif',
    'gridmet20m_all_z_p98_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P98_20m.tif',
    'gridmet20m_all_z_p99_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_P99_20m.tif',
    'gridmet20m_all_z_sd_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_Stddev_20m.tif',
    'gridmet20m_all_z_skew.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_Height_Skew_20m.tif',
    'gridmet20m_density_cc.tif':
        MET20M_DIR + 'SD_BlackHills_D23_CanopyCover_20m.tif',
    'gridmet20m_density_cd.tif':
        MET20M_DIR + 'SD_BlackHills_D23_CanopyDensity_20m.tif',
    'gridmet20m_density_pntdn.tif':
        MET20M_DIR + 'SD_BlackHills_D23_PointDensity_20m.tif',
    'gridmet20m_density_puldn.tif':
        MET20M_DIR + 'SD_BlackHills_D23_PulseDensity_20m.tif',
    'gridmet20m_frst_count.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Count_20m.tif',
    'gridmet20m_frst_kde_peak1_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peak1_Diff_20m.tif',
    'gridmet20m_frst_kde_peak1_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peak1_Elev_20m.tif',
    'gridmet20m_frst_kde_peak1_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peak1_Value_20m.tif',
    'gridmet20m_frst_kde_peak2_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peak2_Diff_20m.tif',
    'gridmet20m_frst_kde_peak2_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peak2_Elev_20m.tif',
    'gridmet20m_frst_kde_peak2_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peak2_Value_20m.tif',
    'gridmet20m_frst_kde_peak3_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peak3_Diff_20m.tif',
    'gridmet20m_frst_kde_peak3_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peak3_Elev_20m.tif',
    'gridmet20m_frst_kde_peak3_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peak3_Value_20m.tif',
    'gridmet20m_frst_kde_peak4_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peak4_Elev_20m.tif',
    'gridmet20m_frst_kde_peak4_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peak4_Value_20m.tif',
    'gridmet20m_frst_kde_peaks_count.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_KDE_Peaks_Count_20m.tif',
    'gridmet20m_frst_L1_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Lmoment_1_20m.tif',
    'gridmet20m_frst_L2_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Lmoment_2_20m.tif',
    'gridmet20m_frst_L3_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Lmoment_3_20m.tif',
    'gridmet20m_frst_L4_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Lmoment_4_20m.tif',
    'gridmet20m_frst_Lcoefvar.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Lmoment_CV_20m.tif',
    'gridmet20m_frst_Lkurt.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Lmoment_Kurt_20m.tif',
    'gridmet20m_frst_Lskew.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Lmoment_Skew_20m.tif',
    'gridmet20m_frst_z_cv.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_CV_20m.tif',
    'gridmet20m_frst_z_kurt.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_Kurt_20m.tif',
    'gridmet20m_frst_z_max_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_Max_20m.tif',
    'gridmet20m_frst_z_mean_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_Mean_20m.tif',
    'gridmet20m_frst_z_min_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_Min_20m.tif',
    'gridmet20m_frst_z_p01_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P01_20m.tif',
    'gridmet20m_frst_z_p02_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P02_20m.tif',
    'gridmet20m_frst_z_p05_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P05_20m.tif',
    'gridmet20m_frst_z_p10_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P10_20m.tif',
    'gridmet20m_frst_z_p20_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P20_20m.tif',
    'gridmet20m_frst_z_p25_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P25_20m.tif',
    'gridmet20m_frst_z_p30_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P30_20m.tif',
    'gridmet20m_frst_z_p40_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P40_20m.tif',
    'gridmet20m_frst_z_p50_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P50_20m.tif',
    'gridmet20m_frst_z_p60_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P60_20m.tif',
    'gridmet20m_frst_z_p70_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P70_20m.tif',
    'gridmet20m_frst_z_p75_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P75_20m.tif',
    'gridmet20m_frst_z_p80_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P80_20m.tif',
    'gridmet20m_frst_z_p90_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P90_20m.tif',
    'gridmet20m_frst_z_p95_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P95_20m.tif',
    'gridmet20m_frst_z_p98_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P98_20m.tif',
    'gridmet20m_frst_z_p99_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_P99_20m.tif',
    'gridmet20m_frst_z_sd_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_Stddev_20m.tif',
    'gridmet20m_frst_z_skew.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_Height_Skew_20m.tif',
    'gridmet20m_frsthp_count.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Count_20m.tif',
    'gridmet20m_frsthp_kde_peak1_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peak1_Diff_20m.tif',
    'gridmet20m_frsthp_kde_peak1_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peak1_Elev_20m.tif',
    'gridmet20m_frsthp_kde_peak1_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peak1_Value_20m.tif',
    'gridmet20m_frsthp_kde_peak2_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peak2_Diff_20m.tif',
    'gridmet20m_frsthp_kde_peak2_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peak2_Elev_20m.tif',
    'gridmet20m_frsthp_kde_peak2_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peak2_Value_20m.tif',
    'gridmet20m_frsthp_kde_peak3_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peak3_Diff_20m.tif',
    'gridmet20m_frsthp_kde_peak3_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peak3_Elev_20m.tif',
    'gridmet20m_frsthp_kde_peak3_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peak3_Value_20m.tif',
    'gridmet20m_frsthp_kde_peak4_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peak4_Elev_20m.tif',
    'gridmet20m_frsthp_kde_peak4_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peak4_Value_20m.tif',
    'gridmet20m_frsthp_kde_peaks_count.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_KDE_Peaks_Count_20m.tif',
    'gridmet20m_frsthp_L1_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Lmoment_1_20m.tif',
    'gridmet20m_frsthp_L2_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Lmoment_2_20m.tif',
    'gridmet20m_frsthp_L3_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Lmoment_3_20m.tif',
    'gridmet20m_frsthp_L4_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Lmoment_4_20m.tif',
    'gridmet20m_frsthp_Lcoefvar.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Lmoment_CV_20m.tif',
    'gridmet20m_frsthp_Lkurt.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Lmoment_Kurt_20m.tif',
    'gridmet20m_frsthp_Lskew.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Lmoment_Skew_20m.tif',
    'gridmet20m_frsthp_z_cv.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_CV_20m.tif',
    'gridmet20m_frsthp_z_kurt.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_Kurt_20m.tif',
    'gridmet20m_frsthp_z_max_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_Max_20m.tif',
    'gridmet20m_frsthp_z_mean_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_Mean_20m.tif',
    'gridmet20m_frsthp_z_min_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_Min_20m.tif',
    'gridmet20m_frsthp_z_p01_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P01_20m.tif',
    'gridmet20m_frsthp_z_p02_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P02_20m.tif',
    'gridmet20m_frsthp_z_p05_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P05_20m.tif',
    'gridmet20m_frsthp_z_p10_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P10_20m.tif',
    'gridmet20m_frsthp_z_p20_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P20_20m.tif',
    'gridmet20m_frsthp_z_p25_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P25_20m.tif',
    'gridmet20m_frsthp_z_p30_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P30_20m.tif',
    'gridmet20m_frsthp_z_p40_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P40_20m.tif',
    'gridmet20m_frsthp_z_p50_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P50_20m.tif',
    'gridmet20m_frsthp_z_p60_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P60_20m.tif',
    'gridmet20m_frsthp_z_p70_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P70_20m.tif',
    'gridmet20m_frsthp_z_p75_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P75_20m.tif',
    'gridmet20m_frsthp_z_p80_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P80_20m.tif',
    'gridmet20m_frsthp_z_p90_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P90_20m.tif',
    'gridmet20m_frsthp_z_p95_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P95_20m.tif',
    'gridmet20m_frsthp_z_p98_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P98_20m.tif',
    'gridmet20m_frsthp_z_p99_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_P99_20m.tif',
    'gridmet20m_frsthp_z_sd_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_Stddev_20m.tif',
    'gridmet20m_frsthp_z_skew.tif':
        MET20M_DIR + 'SD_BlackHills_D23_1stReturns_4p5ftPlus_Height_Skew_20m.tif',
    'gridmet20m_hp_count.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Count_20m.tif',
    'gridmet20m_hp_kde_peak1_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peak1_Diff_20m.tif',
    'gridmet20m_hp_kde_peak1_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peak1_Elev_20m.tif',
    'gridmet20m_hp_kde_peak1_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peak1_Value_20m.tif',
    'gridmet20m_hp_kde_peak2_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peak2_Diff_20m.tif',
    'gridmet20m_hp_kde_peak2_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peak2_Elev_20m.tif',
    'gridmet20m_hp_kde_peak2_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peak2_Value_20m.tif',
    'gridmet20m_hp_kde_peak3_diff_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peak3_Diff_20m.tif',
    'gridmet20m_hp_kde_peak3_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peak3_Elev_20m.tif',
    'gridmet20m_hp_kde_peak3_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peak3_Value_20m.tif',
    'gridmet20m_hp_kde_peak4_elev_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peak4_Elev_20m.tif',
    'gridmet20m_hp_kde_peak4_value_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peak4_Value_20m.tif',
    'gridmet20m_hp_kde_peaks_count.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_KDE_Peaks_Count_20m.tif',
    'gridmet20m_hp_L1_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Lmoment_1_20m.tif',
    'gridmet20m_hp_L2_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Lmoment_2_20m.tif',
    'gridmet20m_hp_L3_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Lmoment_3_20m.tif',
    'gridmet20m_hp_L4_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Lmoment_4_20m.tif',
    'gridmet20m_hp_Lcoefvar.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Lmoment_CV_20m.tif',
    'gridmet20m_hp_Lkurt.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Lmoment_Kurt_20m.tif',
    'gridmet20m_hp_Lskew.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Lmoment_Skew_20m.tif',
    'gridmet20m_hp_z_cv.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_CV_20m.tif',
    'gridmet20m_hp_z_kurt.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_Kurt_20m.tif',
    'gridmet20m_hp_z_max_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_Max_20m.tif',
    'gridmet20m_hp_z_mean_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_Mean_20m.tif',
    'gridmet20m_hp_z_min_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_Min_20m.tif',
    'gridmet20m_hp_z_p01_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P01_20m.tif',
    'gridmet20m_hp_z_p02_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P02_20m.tif',
    'gridmet20m_hp_z_p05_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P05_20m.tif',
    'gridmet20m_hp_z_p10_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P10_20m.tif',
    'gridmet20m_hp_z_p20_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P20_20m.tif',
    'gridmet20m_hp_z_p25_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P25_20m.tif',
    'gridmet20m_hp_z_p30_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P30_20m.tif',
    'gridmet20m_hp_z_p40_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P40_20m.tif',
    'gridmet20m_hp_z_p50_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P50_20m.tif',
    'gridmet20m_hp_z_p60_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P60_20m.tif',
    'gridmet20m_hp_z_p70_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P70_20m.tif',
    'gridmet20m_hp_z_p75_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P75_20m.tif',
    'gridmet20m_hp_z_p80_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P80_20m.tif',
    'gridmet20m_hp_z_p90_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P90_20m.tif',
    'gridmet20m_hp_z_p95_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P95_20m.tif',
    'gridmet20m_hp_z_p98_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P98_20m.tif',
    'gridmet20m_hp_z_p99_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_P99_20m.tif',
    'gridmet20m_hp_z_sd_ft.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_Stddev_20m.tif',
    'gridmet20m_hp_z_skew.tif':
        MET20M_DIR + 'SD_BlackHills_D23_AllReturns_4p5ftPlus_Height_Skew_20m.tif',
    'gridmet20m_strat_all_0000to0005.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_0to0p5ft_20m.tif',
    'gridmet20m_strat_all_0005to0010.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_0p5to1ft_20m.tif',
    'gridmet20m_strat_all_0010to0045.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_1to4p5ft_20m.tif',
    'gridmet20m_strat_all_0045to0100.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_4p5to10ft_20m.tif',
    'gridmet20m_strat_all_0100to0200.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_10to20ft_20m.tif',
    'gridmet20m_strat_all_0200to0300.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_20to30ft_20m.tif',
    'gridmet20m_strat_all_0300to0400.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_30to40ft_20m.tif',
    'gridmet20m_strat_all_0400to0500.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_40to50ft_20m.tif',
    'gridmet20m_strat_all_0500to0600.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_50to60ft_20m.tif',
    'gridmet20m_strat_all_0600to0700.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_60to70ft_20m.tif',
    'gridmet20m_strat_all_0700to0800.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_70to80ft_20m.tif',
    'gridmet20m_strat_all_0800to0900.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_80to90ft_20m.tif',
    'gridmet20m_strat_all_0900to1000.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_90to100ft_20m.tif',
    'gridmet20m_strat_all_1000to1100.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_100to110ft_20m.tif',
    'gridmet20m_strat_all_1100to1200.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_110to120ft_20m.tif',
    'gridmet20m_strat_all_1200to1300.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_120to130ft_20m.tif',
    'gridmet20m_strat_all_1300to1400.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_130to140ft_20m.tif',
    'gridmet20m_strat_all_1400to1500.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_140to150ft_20m.tif',
    'gridmet20m_strat_all_1500to1600.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_150to160ft_20m.tif',
    'gridmet20m_strat_all_1600to1700.tif':
        STRAT_DIR + 'SD_BlackHills_D23_AllReturns_Strata_160to170ft_20m.tif',
    'gridmet20m_strat_frst_0000to0005.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_0to0p5ft_20m.tif',
    'gridmet20m_strat_frst_0005to0010.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_0p5to1ft_20m.tif',
    'gridmet20m_strat_frst_0010to0045.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_1to4p5ft_20m.tif',
    'gridmet20m_strat_frst_0045to0100.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_4p5to10ft_20m.tif',
    'gridmet20m_strat_frst_0100to0200.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_10to20ft_20m.tif',
    'gridmet20m_strat_frst_0200to0300.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_20to30ft_20m.tif',
    'gridmet20m_strat_frst_0300to0400.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_30to40ft_20m.tif',
    'gridmet20m_strat_frst_0400to0500.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_40to50ft_20m.tif',
    'gridmet20m_strat_frst_0500to0600.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_50to60ft_20m.tif',
    'gridmet20m_strat_frst_0600to0700.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_60to70ft_20m.tif',
    'gridmet20m_strat_frst_0700to0800.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_70to80ft_20m.tif',
    'gridmet20m_strat_frst_0800to0900.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_80to90ft_20m.tif',
    'gridmet20m_strat_frst_0900to1000.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_90to100ft_20m.tif',
    'gridmet20m_strat_frst_1000to1100.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_100to110ft_20m.tif',
    'gridmet20m_strat_frst_1100to1200.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_110to120ft_20m.tif',
    'gridmet20m_strat_frst_1200to1300.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_120to130ft_20m.tif',
    'gridmet20m_strat_frst_1300to1400.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_130to140ft_20m.tif',
    'gridmet20m_strat_frst_1400to1500.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_140to150ft_20m.tif',
    'gridmet20m_strat_frst_1500to1600.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_150to160ft_20m.tif',
    'gridmet20m_strat_frst_1600to1700.tif':
        STRAT_DIR + 'SD_BlackHills_D23_1stReturns_Strata_160to170ft_20m.tif',
}


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def cd0() -> None:
    """Change directory to the parent path of this script file."""

    this_dir = sys.path[0]  # Dir where this script resides
    os.chdir(this_dir)


def print2(*args, **kwargs) -> None:
    """Print message to stderr."""

    print(*args, **kwargs, file=sys.stderr)


def timestamp() -> str:
    """Return current date and time as string."""

    return datetime.now().strftime("%Y-%m-%d %H:%M")


def lsdir(d: Path) -> Iterator[Path]:
    """Return iterable of paths for the given directory (sans hidden files)."""

    yield from d.glob("[!.]*")


def get_path_stem(path: Path) -> str:
    """Remove all filename extensions and parent path.

    Python's pathlib .stem method only removes one file extension. This
    function removes them all.
    """

    path = Path(path)
    while True:
        path_stem = Path(path.stem)
        if path == path_stem:
            return str(path_stem)
        path = path_stem


def init_logger(
    path: Path = None, level=logging.INFO, capture_exceptions=True,
) -> logging.Logger:
    """Initialize logger, set format, and set verbosity level."""

    fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    logger = logging.getLogger()
    logger.setLevel(level)

    # Set logger to output to stderr
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    # Set logger to output to a file as well
    if path:
        file = logging.FileHandler(path, mode='a')
        file.setFormatter(fmt)
        logger.addHandler(file)

    # Capture unhandled exceptions
    if capture_exceptions:
        sys.excepthook = lambda e_type, e_val, e_traceback: logger.critical(
            "Program terminating:", exc_info=(e_type, e_val, e_traceback)
        )

    return logger


def papply(jobs: iter, worker: Callable, max_workers=os.cpu_count()) -> list:
    """Run jobs in parallel.

    Parallel apply dispatching function: Run a list of jobs with a given
    worker function, in parallel, and return the results in order of jobs
    list. Uses concurrent.futures as backend.
    """
    futures = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, j) for j in jobs]
        results = [f.result() for f in futures]

    return results


def dispatch(
    jobs: iter, worker: Callable, max_workers=os.cpu_count(),
) -> Iterator[(Any, Any)]:
    """Run jobs in parallel.

    Parallel apply dispatching function: Run a list of jobs with a given
    worker function, in parallel. Yield worker results in order they
    finish.
    """

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures_jobs = {executor.submit(worker, j): j for j in jobs}
        for f in cf_as_completed(futures_jobs):
            yield (futures_jobs[f], f.result())


def apply_naming_scheme(tif_dir: Path) -> None:
    """Organize grid metrics TIFF files.

    Rename and reorganize grid metrics TIFF files to conform to the naming
    scheme the Forest Service requested.
    """

    for src_name in NAMING_MAP:
        src_tif = Path(tif_dir, src_name)
        dst_tif = Path(tif_dir, NAMING_MAP[src_name])
        dst_tif.parent.mkdir(parents=True, exist_ok=True)
        try:
            src_tif.rename(dst_tif)
        except FileNotFoundError:
            print("warning: File not found:", src_tif)
        else:
            print(src_tif, '  --->  ', dst_tif)
