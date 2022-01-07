# Munging driver data from geoknife to create NetCDF for each GCM.

#' @title Creates a NetCDF file with GCM driver data
#' @description For each of the GCMs, create a single NetCDF file containing all the
#' variables and all the grid cells.
#' @param nc_file netcdf file name output
#' @param gcm_raw_files vector of feather file paths, one for each tile with data for the current GCM.
#' @param dim_time_input vector of GCM driver data dates
#' @param dim_cell_input vector of all GCM driver grid cells (whether or not data was pulled)
#' @param vars_info variables and descriptions to store in NetCDF
#' @param global_att global attribute description for the observations (e.g. notaro_ACCESS_1980_1999)
#' @param overwrite T/F if the nc file should be overwritten if it exists already
generate_gcm_nc <- function(nc_file, gcm_raw_files, dim_time_input, dim_cell_input,
                            vars_info, global_att, overwrite = TRUE) {

  # Use `ncdfgeom` to write out the NetCDF DSG file
  # See example at https://github.com/jread-usgs/lakesurf-data-release/blob/main/src/ncdf_utils.R#L66-L76
  # And more discussion in https://github.com/USGS-R/lake-temperature-model-prep/issues/252
  # ncdfgeom::write_timeseries_dsg(nc_file)

  # TODO: remove this step which just creates an empty file
  # with a .nc extension as a placeholder
  writeLines("PLACEHOLDER", nc_file)

  return(nc_file)
}

