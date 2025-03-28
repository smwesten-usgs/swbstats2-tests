import xarray as xr
import rioxarray as rio
import xrspatial as xrs
import numpy as np
import pandas as pd
import datetime as dt
from pyproj import CRS 
#
# Information from Ryan Noe:
#
# This folder contains CMIP6 derived data products in .tif format. Each subfolder at the root level is a separate model.
# Parsing the filename requires some care because there are optional values and sometimes multiple units. 
# Each raster is named according the following convention:

# {SCENARIO}_{MODEL}_{OUTPUTTYPE}_{TIMEFRAME}-{SUBTIMEFRAME}_{VARIABLE-UNIT-DESCRIPTION}_{THRESHOLD-UNIT}.tif

# In the simplest case:
# historical_1983-1996_ensemble_modelVal_yearly_precip-inches.tif

# In an advanced case:
# RCP4.5_2043-2056_ensemble_numDif_slice-0501-0930_precip-days-max-consecutive-below_0.01-inch.tif

num_seasons = {12: 'winter', 3: 'spring', 6: 'summer', 9: 'fall'}
seasons = {'12': 'winter','03': 'spring','06': 'summer','09': 'fall'}
mn_seasons = {'12': 'seasonal-DJF', '03': 'seasonal-MAM', '06': 'seasonal-JJA', '09': 'seasonal-SON'}

def export_xarray_dataset_as_series_of_tif_images(ds,
                                            summary_basetype,
                                            variable_operation,
                                            scenario_name,
                                            weather_data_name,
                                            swb_variable_name,
                                            time_period,
                                            output_image_dir,
                                            from_epsg=5070,
                                            to_epsg=4326):


  ds.rio.write_crs(CRS.from_epsg(from_epsg),inplace=True)

  SCENARIO = f"{scenario_name}_{time_period}"
  MODEL = f"{weather_data_name}"
  OUTPUTTYPE = "modelVal"
  UNITS = ds.units


  match summary_basetype:

    case "seasonal":

        for i in range(len(ds.time.values)):
            seasonal = ds.isel(time=i).rio.reproject(f"EPSG:{to_epsg}")
            month_val = f"{seasonal.month:02d}"
            # try:
            #    season_name = seasons[month_val]
            #    seasonal.rio.to_raster(output_image_dir / f"{time_period}__{summary_basetype}_{variable_operation}_{season_name}__{scenario_name}__{weather_data_name}__{swb_variable_name}.tif",
            #                           driver="GTiff", compress="LZW")
            # except:
            #    exit(f"'month_val' is {month_val}. There were problems")
            try:
               TIMEFRAME = mn_seasons[month_val]
               filename = f"{SCENARIO}_{MODEL}_{OUTPUTTYPE}_{TIMEFRAME}_{swb_variable_name}-{UNITS}.tif"
               seasonal.rio.to_raster(output_image_dir / filename,
                                      driver="GTiff", compress="LZW")
            except:
               exit(f"'TIMEFRAME' is {TIMEFRAME}. There were problems")

        return

    case "mean_seasonal":

        for i in ds.month.values:
            seasonal = ds.sel(month=i).rio.reproject(f"EPSG:{to_epsg}")
            month_val = f"{seasonal.month:02d}"
            # try:
            #    season_name = seasons[month_val]
            #    seasonal.rio.to_raster(output_image_dir / f"{time_period}__{summary_basetype}_{variable_operation}_{season_name}__{scenario_name}__{weather_data_name}__{swb_variable_name}.tif",
            #                           driver="GTiff", compress="LZW")
            # except:
            #    exit(f"'month_val' is {month_val}. There were problems")
            try:
               TIMEFRAME = mn_seasons[month_val]
               filename = f"{SCENARIO}_{MODEL}_{OUTPUTTYPE}_{TIMEFRAME}_{swb_variable_name}-{UNITS}.tif"
               seasonal.rio.to_raster(output_image_dir / filename,
                                      driver="GTiff", compress="LZW")
            except:
               exit(f"'TIMEFRAME' is {TIMEFRAME}. There were problems")
        return

    case "mean_annual":
      mean_annual = ds.rio.reproject(f"EPSG:{to_epsg}")
      TIMEFRAME = "yearly"  
      filename = f"{SCENARIO}_{MODEL}_{OUTPUTTYPE}_{TIMEFRAME}_{swb_variable_name}-{UNITS}.tif"        
      mean_annual[swb_variable_name].rio.to_raster(output_image_dir / filename,
                                                   driver="GTiff", compress="LZW")
      return

    case "annual":

        for i in range(len(ds.time.values)):
            yearly = ds.isel(time=i).rio.reproject(f"EPSG:{to_epsg}")
            year_val = str(yearly.time.values).split('-')[0]
            TIMEFRAME = "yearly"
            filename = f"{SCENARIO}_{MODEL}_{OUTPUTTYPE}_{TIMEFRAME}_{swb_variable_name}-{UNITS}.tif"
            yearly.rio.to_raster(output_image_dir / filename,
                                 driver="GTiff", compress="LZW")
        return

    case _:
          print(f"export_xarray_dataset_as_series_of_tif_images: unknown summary_basetype '{summary_basetype}'")
          #exit(1)


def export_xarray_dataset_as_netcdf(ds,
                                    output_grid_name):


    ds.to_netcdf(output_grid_name)


def export_zonal_stats_dataframe_as_parquet(df,
                                            summary_basetype,
                                            variable_operation,
                                            scenario_name,
                                            weather_data_name,
                                            swb_variable_name,
                                            time_period,
                                            data_summary_dir):

  match summary_basetype:

    case "seasonal":

      df['season_name'] = df.month
      df.replace({'season_name': num_seasons}, inplace=True)
      df.to_parquet(path=data_summary_dir / 
        f"{time_period}__{summary_basetype}_{variable_operation}__{scenario_name}__{weather_data_name}__{swb_variable_name}.parquet")
      return

    case "mean_seasonal":
      df['season_name'] = df.month
      df.replace({'season_name': num_seasons}, inplace=True)
      df.to_parquet(path=data_summary_dir / 
        f"{time_period}__{summary_basetype}_{variable_operation}__{scenario_name}__{weather_data_name}__{swb_variable_name}.parquet")
      return

    case "mean_annual":
      df.to_parquet(path=data_summary_dir / 
        f"{time_period}__{summary_basetype}_{variable_operation}__{scenario_name}__{weather_data_name}__{swb_variable_name}.parquet")
      return

    case "annual":
      df.to_parquet(path=data_summary_dir / 
        f"{time_period}__{summary_basetype}_{variable_operation}__{scenario_name}__{weather_data_name}__{swb_variable_name}.parquet")
      return

    case _:
      print(f"export_zonal_stats_dataframe_as_parquet: unknown summary_basetype '{summary_basetype}'")
