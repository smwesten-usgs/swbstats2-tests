import xarray as xr
import rioxarray as rio
import xrspatial as xrs
import numpy as np
import pandas as pd
import datetime as dt

month_to_growing_season_lu = np.array([None,
                                      'nongrowing',
                                      'nongrowing',
                                      'nongrowing',
                                      'nongrowing',
                                      'growing',
                                      'growing',
                                      'growing',
                                      'growing',
                                      'growing',
                                      'nongrowing',
                                      'nongrowing',
                                      'nongrowing',
                                      ])

def create_summary_dataset(netcdf_filename,
                           scenario_name,
                           swb_variable_name, 
                           weather_data_name, 
                           summary_basetype='none',
                           variable_operation='none',
                           startdate=None,
                           enddate=None):

    xarray_dataset = xr.open_dataset(netcdf_filename, 
                                     decode_coords=True, 
                                     decode_cf=True)

    if startdate is not None:
        xarray_dataset = xarray_dataset.sel(time=slice(startdate, enddate))

    units = xarray_dataset[f"{swb_variable_name}"].units
# todo: resample is apparently just a fancy wrapper around 'groupby'; reformulating these to groupby 'water_year' 
# should be possible

    summary_type = f"{summary_basetype}_{variable_operation}"
    
    match summary_type:

        case 'mean_seasonal_sum':
            # return 4 grids of summed daily gridded values
            # returns stats for DJF, MAM, JJA, SON
            result_dataset = xarray_dataset.resample(time="QS-DEC").reduce(np.sum, dim="time").groupby("time.month").reduce(np.nanmean, dim="time")

        case 'seasonal_sum':
            # return 'n/4' grids of summed daily gridded values with 'n/4' equal to the number of quarters in the input dataset
            # returns stats for DJF, MAM, JJA, SON
            result_dataset = xarray_dataset.resample(time="QS-DEC").reduce(np.sum, dim="time")

        case 'seasonal_mean':
            # return 'n/4' grids of averaged daily gridded values with 'n/4' equal to the number of quarters in the input dataset
            result_dataset = xarray_dataset.resample(time="QS-DEC").mean(dim="time",)

        case 'mean_seasonal_mean':
            # return 4 grids of averaged daily gridded values
            result_dataset = xarray_dataset.resample(time="QS-DEC").reduce(np.mean, dim="time").groupby("time.month").reduce(np.nanmean, dim="time")

        case 'monthly_sum':     
            # return 'n' grids of summed daily gridded values with 'n' equal to the number of months in the input dataset    
            result_dataset = xarray_dataset.resample(time="ME").reduce(np.nansum, dim="time")

        case 'monthly_mean':
            # return 'n' grids of averaged daily gridded values with 'n' equal to the number of months in the input dataset    
            result_dataset = xarray_dataset.resample(time="ME").reduce(np.nanmean, dim="time")

        case 'mean_monthly_sum':
            # return 12 grids of summed daily gridded values; each grid represents the mean of the 
            #   sum of all January values, February values, etc.    
            result_dataset = xarray_dataset.resample(time="ME").reduce(np.nansum, dim="time").groupby("time.month").reduce(np.nanmean, dim="time")

        case 'mean_monthly_mean':
            # return 12 grids of averaged daily gridded values; each grid represents the mean of the 
            #   mean of all January values, February values, etc.    
            result_dataset = xarray_dataset.resample(time="ME").reduce(np.nanmean, dim="time").groupby("time.month").reduce(np.nanmean, dim="time")

        case 'annual_sum':
            # return 'n' grids of summed daily gridded values, with 'n' equal to the number of years in the input dataset
            #result_dataset = xarray_dataset.resample(time="A").sum(dim="time")
            result_dataset = xarray_dataset.resample(time="YE").sum(dim='time', skipna=True)

        case 'annual_mean':
            # return 'n' grids of averaged daily gridded values, with 'n' equal to the number of years in the input dataset
            #result_dataset = xarray_dataset.resample(time="A").mean(dim="time")
            result_dataset = xarray_dataset.resample(time="YE").mean(dim="time", skipna=True)

        case 'mean_annual_sum':
            # return a single grid representing the mean of each annual summed variable amount over all years
            # ==> .sum(dim='time', skipna=True) *should* result in a resampled grid that respects NaNs; however, this oes not appear to work at the moment
            result_dataset = xarray_dataset.resample(time="YE").sum(dim="time", skipna=True).mean(dim="time", skipna=True)

        case 'mean_annual_mean':
            # return a single grid representing the mean of each annual mean variable amount over all years
            result_dataset = xarray_dataset.resample(time="YE").mean(dim="time", skipna=True).mean(dim="time", skipna=True)

        case _:
              print(f"unknown calculation_type '{summary_basetype}'")
              exit(1)

    result_dataset = result_dataset.assign_attrs(swb_variable_name=swb_variable_name, 
                                                 summary_basetype=summary_basetype,
                                                 variable_operation=variable_operation,
                                                 weather_data_name=weather_data_name,
                                                 scenario_name=scenario_name,
                                                 units=units,
                                                 original_source_filename=str(netcdf_filename))

    xarray_dataset.close()
    # if crs is not None:
    #     result_dataset.rio.write_crs(crs)
    #     result_dataset['crs'] = crs

    return result_dataset