import logging
import multiprocessing
import os
import time
from pathlib import Path
from typing import Tuple
from functools import partial, cache
from datetime import datetime

import dask.delayed
import dask.delayed
import numba
import numpy as np
import dask
import geopandas as gpd
import pandas as pd
import s3fs
import xarray as xr
from exactextract import exact_extract

from dask.distributed import Client, get_client

from data_processing.file_paths import file_paths
from dask.distributed import get_client, TimeoutError

from tqdm import tqdm

logger = logging.getLogger(__name__)
client = get_client("localhost:8786", timeout=5)


def load_zarr_datasets() -> xr.Dataset:
    """Load zarr datasets from S3 within the specified time range."""
    forcing_vars = ["lwdown", "precip", "psfc", "q2d", "swdown", "t2d", "u2d", "v2d"]
    s3_urls = [
        f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/{var}.zarr"
        for var in forcing_vars
    ]
    s3_stores = [s3fs.S3Map(url, s3=s3fs.S3FileSystem(anon=True)) for url in s3_urls]
    dataset = xr.open_mfdataset(s3_stores, parallel=True, chunks="auto", engine="zarr")
    return dataset


def load_geodataframe(geopackage_path: str, projection: str) -> gpd.GeoDataFrame:
    """Load and project a geodataframe from a given path and projection."""
    gdf = gpd.read_file(geopackage_path, layer="divides").to_crs(projection)
    return gdf


def clip_dataset_to_bounds(
    dataset: xr.Dataset, bounds: Tuple[float, float, float, float], start_time: str, end_time: str
) -> xr.Dataset:
    """Clip the dataset to specified geographical bounds."""
    dataset = dataset.sel(
        x=slice(bounds[0], bounds[2]),
        y=slice(bounds[1], bounds[3]),
        time=slice(start_time, end_time),
    )
    logger.info("Selected time range and clipped to bounds")
    return dataset


def compute_store(stores: xr.Dataset, cached_nc_path: Path) -> xr.Dataset:
    print("Computing store")
    stores.drop_vars("crs")
    rechunked = stores.chunk({"time": None, "y": "auto", "x": "auto"})
    # rechunked to optimize for numba time series operations
    rechunked.to_netcdf(cached_nc_path)
    # this seems insane but it stops future requests from redownloading data from s3
    rechunked = xr.open_mfdataset(cached_nc_path, parallel=True, chunks="auto")
    return rechunked


@numba.jit(parallel=True)
def dask_weighted_mean(data_array, cell_ids, weights):
    mean_at_timestep = np.zeros(data_array.shape[0])
    for time_step in numba.prange(data_array.shape[0]):
        weighted_total = 0.0
        for i in range(cell_ids.shape[0]):
            weighted_total += data_array[time_step][cell_ids[i]] * weights[i]
        mean_at_timestep[time_step] = weighted_total / weights.sum()
    return mean_at_timestep


@dask.delayed
def get_cell_weights(raster, gdf):
    output = exact_extract(
        raster["LWDOWN"],
        gdf,
        ["cell_id", "coverage"],
        include_cols=["divide_id"],
        output="pandas",
    )
    return output.set_index("divide_id")


def add_APCP_SURFACE_to_dataset(dataset: xr.Dataset) -> xr.Dataset:
    dataset["APCP_surface"] = (dataset["RAINRATE"] * 3600 * 1000) / 0.998
    return dataset


@dask.delayed
def setup_delayed_compute(data, cell_ids, weights, catchment, output_folder):
    variables = [
        "LWDOWN",
        "PSFC",
        "Q2D",
        "RAINRATE",
        "SWDOWN",
        "T2D",
        "U2D",
        "V2D",
        "APCP_surface",
    ]
    # Rename variables
    renamed_vars = {
        "LWDOWN": "DLWRF_surface",
        "PSFC": "PRES_surface",
        "Q2D": "SPFH_2maboveground",
        "RAINRATE": "precip_rate",
        "SWDOWN": "DSWRF_surface",
        "T2D": "TMP_2maboveground",
        "U2D": "UGRD_10maboveground",
        "V2D": "VGRD_10maboveground",
        "APCP_surface": "APCP_surface",
    }
    delayed_saves = []
    for variable in variables:
        # Load the variable data in chunks
        variable_data = []
        raster = data[variable].values.reshape(data[variable].shape[0], -1)
        mean_at_timesteps = dask.delayed(dask_weighted_mean)(raster, cell_ids, weights)
        variable_data.append(mean_at_timesteps)
        # Concatenate the chunked results
        mean_at_timesteps = dask.delayed(np.concatenate)(variable_data)
        # Create a delayed DataArray
        temp_da = dask.delayed(xr.DataArray)(
            mean_at_timesteps,
            dims=["time"],
            coords={"time": data["time"].values},
            name=f"{renamed_vars[variable]}_{catchment}",
        )
        temp_da = dask.delayed(temp_da.assign_coords)(catchment=catchment)

        # Save the delayed DataArray to disk
        csv_path = output_folder / f"{catchment}_{renamed_vars[variable]}.csv"
        delayed_save = dask.delayed(temp_da.to_dataframe().to_csv)(csv_path)
        delayed_saves.append(delayed_save)
    return delayed_saves


def compute_zonal_stats(
    gdf: gpd.GeoDataFrame, merged_data: xr.Dataset, forcings_dir: Path
) -> None:
    logger.info("Computing zonal stats in parallel for all timesteps")
    timer_start = time.time()
    gfd_chunks = np.array_split(gdf, multiprocessing.cpu_count() - 1)
    one_timestep = merged_data.isel(time=0).compute()

    for gdf_chunk in gfd_chunks:
        catchments = get_cell_weights(one_timestep, gdf_chunk)

    catchments = dask.compute(catchments)
    catchments = pd.concat(catchments)

    # Add APCP_SURFACE to the dataset
    merged_data["APCP_surface"] = (merged_data["RAINRATE"] * 3600 * 1000) / 0.998

    output_folder = forcings_dir / "by_catchment"
    # Clear out any existing files
    for file in output_folder.glob("*.csv"):
        file.unlink()

    # scatter the merged data
    client.scatter(merged_data)
    print("Scattered data")

    delayed_saves = []
    for catchment in tqdm(catchments.index.unique()):
        cell_ids = catchments.loc[catchment]["cell_id"]
        weights = catchments.loc[catchment]["coverage"]
        delayed_saves.append(
            setup_delayed_compute(merged_data, cell_ids, weights, catchment, output_folder)
        )

    print("Computing delayed saves")
    # Compute all delayed operations
    dask.compute(delayed_saves)

    logger.info(
        f"Forcing generation complete!\nZonal stats computed in {time.time() - timer_start} seconds"
    )


def setup_directories(wb_id: str) -> file_paths:
    forcing_paths = file_paths(wb_id)

    for folder in ["by_catchment", "temp"]:
        os.makedirs(forcing_paths.forcings_dir() / folder, exist_ok=True)
    return forcing_paths


def create_forcings(start_time: str, end_time: str, output_folder_name: str) -> None:
    forcing_paths = setup_directories(output_folder_name)
    projection = xr.open_dataset(forcing_paths.template_nc()).crs.esri_pe_string
    logger.info("Got projection from grid file")

    gdf = load_geodataframe(forcing_paths.geopackage_path(), projection)
    logger.info("Got gdf")

    if type(start_time) == datetime:
        start_time = start_time.strftime("%Y-%m-%d %H:%M")
    if type(end_time) == datetime:
        end_time = end_time.strftime("%Y-%m-%d %H:%M")

    merged_data = None
    if os.path.exists(forcing_paths.cached_nc_file()):
        logger.info("found cached nc file")
        # open the cached file and check that the time range is correct
        cached_data = xr.open_mfdataset(
            forcing_paths.cached_nc_file(), parallel=True, engine = "netcdf4"
        )
        print(cached_data)
        if cached_data.time[0].values <= np.datetime64(start_time) and cached_data.time[
            -1
        ].values >= np.datetime64(end_time):
            logger.info("Time range is correct")
            logger.info(f"Opened cached nc file: [{forcing_paths.cached_nc_file()}]")
            merged_data = clip_dataset_to_bounds(
                cached_data, gdf.total_bounds, start_time, end_time
            )
            logger.info("Clipped stores")
        else:
            logger.info("Time range is incorrect")
            os.remove(forcing_paths.cached_nc_file())
            logger.info("Removed cached nc file")

    if merged_data is None:
        logger.info("Loading zarr stores, this may take a while.")
        lazy_store = load_zarr_datasets()
        logger.info("Got zarr stores")

        clipped_store = clip_dataset_to_bounds(lazy_store, gdf.total_bounds, start_time, end_time)
        logger.info("Clipped stores")

        merged_data = compute_store(clipped_store, forcing_paths.cached_nc_file())
        logger.info("Computed store")

    logger.info("Computing zonal stats")
    compute_zonal_stats(gdf, merged_data, forcing_paths.forcings_dir())


if __name__ == "__main__":
    # Example usage
    start_time = "2010-01-01 00:00"
    end_time = "2010-01-02 00:00"
    output_folder_name = "wb-1643991"
    # looks in output/wb-1643991/config for the geopackage wb-1643991_subset.gpkg
    # puts forcings in output/wb-1643991/forcings
    logger.basicConfig(level=logging.DEBUG)
    create_forcings(start_time, end_time, output_folder_name)
