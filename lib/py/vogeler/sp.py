#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Author: Daniel Rode
# Dependencies:
#   git
#   gdalbuildvrt
#   gdaladdo
#   gdalwarp
#   pdal
#   pdal_wrench
#   fd


"""Functions that depend on third-party system binaries."""


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Import libraries
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

# Import standard libraries
import json
import subprocess as sp
from pathlib import Path
from tempfile import NamedTemporaryFile
from tempfile import TemporaryDirectory

# Import external libraries
import rasterio as rio
from geopandas import GeoDataFrame


# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------

def import_scripts(
    repo_path: Path, repo_commit: str, repo_assets: list[Path], dst_dir: Path,
) -> None:
    """Copy dependency scripts to another directory using Git."""

    log.info(f"Git repo path: {repo_path}")
    log.info(f"Git repo commit: {repo_commit}")

    dst_dir.mkdir(parents=True, exist_ok=True)

    git_cmd = (
        'git',
        '-C', repo_path,
        'archive',
        '--format', 'tar',
        repo_commit,
        *repo_assets,
    )
    tar_cmd = (
        'tar',
        '--directory', str(dst_dir),
        '--file', '-',
        '--extract',
    )
    cmd_pipe([git_cmd, tar_cmd])


def cmd_pipe(cmds, stdin=None) -> None:
    """Chain system commands.

    Run several system commands and pipe the output from one to the stdin
    of the next.
    """

    # Chain stdin and stdout of given commands together
    proc_list = []
    last_stdout = stdin
    for c in cmds:
        p = sp.Popen(c, stdin=last_stdout, stdout=sp.PIPE)
        proc_list.append(p)
        last_stdout = p.stdout

    # Print output of the last program in the chain
    while p.poll() is None:
        print(str(p.stdout.readline(), 'utf8'), end='')

    # Run commands and make sure none fail
    # stdout, stderr = p.communicate(input=stdin)
    for p in proc_list:
        p.wait()
        if p.returncode != 0:
            raise Exception("Subprocess command failed:", p.args)

    # if stdout:
    #     stdout = str(stdout, 'utf8').strip()
    # if stderr:
    #     stderr = str(stderr, 'utf8').strip()

    # return stdout, stderr


def gdaladdo(tif_path: Path) -> None:
    """Generate pyramids for the given raster."""

    sp.run(['gdaladdo', str(tif_path)], check=True)


def gdal_build_vrt(
    src_paths: list[Path],
    dst_pth: Path,
    pyramids=False,
) -> None:
    """Create virtual mosaic from a list of rasters using GDAL."""

    with NamedTemporaryFile(suffix='.txt', buffering=0) as f:
        # Write list of source rasters to file
        for p in src_paths:
            f.write(bytes(p))
            f.write(b'\n')

        # Run GDAL to build virtual raster from source rasters
        cmd = ['gdalbuildvrt', '-input_file_list', f.name, dst_pth]
        sp.run(cmd, check=True)

    # If requested, generate pyramids for mosaic
    if pyramids:
        gdaladdo(dst_pth)


def vrt2tif(vrt_path: Path, out_path: Path, big=False) -> None:
    """Convert a virtual raster mosaic into a real mosaic using GDAL."""

    # Setup command arguments
    cmd = [
        'gdal_translate',
        '-co', 'COMPRESS=LZW',
        '-co', 'TILED=YES',
    ]
    if big:
        cmd += ['-co', 'BIGTIFF=YES']

    # Run GDAL
    cmd += [str(vrt_path), str(out_path)]
    sp.run(cmd, check=True)


def build_vpc(in_paths: list[Path], out_path: Path) -> None:
    """Create virtual mosaic from a list of point cloud files.

    Uses PDAL Wrench.
    """

    with TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Save list of file paths to temp file to pass to PDAL Wrench
        assets_list_txt = tmpdir / "assets.txt"
        with assets_list_txt.open('w') as f:
            for i in in_paths:
                f.write(str(i))
                f.write("\n")

        # Run PDAL Wrench to build VPC
        cmd = (
            'pdal_wrench', 'build_vpc',
            '--input-file-list', assets_list_txt,
            '--output', out_path,
        )
        sp.run(cmd, check=True)


def find(
    query: str,
    dir_list: list[Path],
    ignore_case=False,
    result_type=None | list[str],
    fixed_strs=False,
) -> list[Path]:
    """List files matching the given query in the given directories."""

    cmd = ['fd']
    if ignore_case:
        cmd += ['--ignore-case']
    if result_type:
        cmd += ['--type', *result_type]
    if fixed_strs:
        cmd += ['--fixed-strings']
    cmd += [str(query), *[str(d) for d in dir_list]]

    proc = sp.run(cmd, check=True, text=True, capture_output=True)

    return [Path(p) for p in proc.stdout.strip().splitlines()]


def clip_rast(in_rast: Path, out_rast: Path, bounds: GeoDataFrame) -> None:
    """Clip a given raster to a given polygon."""

    # Get raster CRS
    with rio.open(in_rast, 'r') as f:
        rast_crs = f.crs

    with TemporaryDirectory() as tmpdir:
        # Save bounds polygon to file so GDAL utility can read it
        bounds_path = Path(tmpdir, "bounds.fgb")
        bounds.to_crs(rast_crs).to_file(bounds_path)

        # Run GDAL to do clipping
        cmd = (
            'gdalwarp',
            '-cutline',
            bounds_path,
            '-crop_to_cutline',
            '-dstalpha',
            '-co', 'BIGTIFF=YES',
            '-co', 'COMPRESS=LZW',
            '-co', 'TILED=YES',
            str(in_rast),
            str(out_rast),
        )
        sp.run(cmd, check=True)


def get_las_crs(las_path: Path) -> str:
    cmd = (
        'pdal',
        'info',
        '--metadata',
        str(las_path),
    )
    p = sp.run(cmd, check=True, text=True, capture_output=True)

    return json.loads(p.stdout)['metadata']['srs']['compoundwkt']
