# Munging driver data from geoknife to create NetCDF for each GCM.

#' @title Creates a NetCDF file with GCM driver data
#' @description For each of the GCMs, create a single NetCDF file containing all the
#' variables and all the grid cells.
#' @param nc_file netcdf file name output
#' @param gcm_raw_files vector of feather file paths, one for each tile with data for the current GCM.
#' @param dim_time_input vector of GCM driver data dates
#' @param dim_cell_input vector of all GCM driver grid cells (whether or not data was pulled)
#' @param vars_info variables and descriptions to store in NetCDF
#' @param crs_info of GCMs - NEED TO FIGURE OUT HOW TO ADD TO NETCDF
#' @param spatial_info cell_nos, x indices, y indices, and projected coordinates of grid cell centroids
#' @param global_att global attribute description for the observations (e.g. notaro_ACCESS_1980_1999)
#' @param overwrite T/F if the nc file should be overwritten if it exists already
generate_gcm_nc <- function(nc_file, gcm_raw_files, dim_time_input, dim_cell_input,
                            vars_info, crs_info, spatial_info, global_att, overwrite = TRUE) {

  # Use `ncdfgeom` to write out the NetCDF DSG file
  # See example at https://github.com/jread-usgs/lakesurf-data-release/blob/main/src/ncdf_utils.R#L66-L76
  # And more discussion in https://github.com/USGS-R/lake-temperature-model-prep/issues/252
  # ncdfgeom::write_timeseries_dsg(nc_file)

  # Read in all data for that GCM and pivot to long format
  gcm_data_long <- purrr::map_df(gcm_raw_files, function(gcm_raw_file) {
    arrow::read_feather(gcm_raw_file)
  }) %>%
    pivot_longer(cols = -c(time, cell), names_to = "variable", values_to = "value")

  # unique cells
  gcm_cells <- unique(gcm_data_long$cell) #' @param instance_names \code{character} or \code{numeric} vector of names for each instance (e.g. station or geometry) to be added to the file.

  # get cell coordinates
  cell_lon_lat <- spatial_info %>%
    mutate(lon = sf::st_coordinates(.)[,1], lat = sf::st_coordinates(.)[,2]) %>%
    rename(cell = cell_no) %>%
    sf::st_set_geometry(NULL) %>%
    filter(cell %in% gcm_cells)

  cell_x <- cell_lon_lat %>% pull(x)
  cell_y <- cell_lon_lat %>% pull(y)
  lats <- cell_lon_lat %>% pull(lat) #' @param lats \code{numeric} vector of latitudes
  lons <- cell_lon_lat %>% pull(lon) #' @param lons \code{numeric} vector of longitudes

  times <- dim_time_input #' @param times \code{POSIXct} vector of times. Must be of type \code{POSIXct} or an attempt to convert it will be made using \code{as.POSIXct(times)}.

  if (file.exists(nc_file))
    unlink(nc_file)

  for (variable in vars_info$var_name) {
    variable_metadata <- vars_info %>% filter(var_name == variable)
    var_data <- gcm_data_long %>%
      filter(variable == variable) %>%
      pivot_wider(id_cols = c("time"), names_from = cell, values_from = value) %>% #' @param data \code{data.frame} with each column corresponding to an instance. Rows correspond to time steps. nrow must be the same length as times. Column names must match instance names.
      select(-time) %>%
      setNames(gcm_cells)

    message(sprintf('variable: %s', variable))
    message(sprintf('cell length: %s', length(gcm_cells)))
    message(sprintf('lat length: %s', length(lats)))
    message(sprintf('lon length: %s', length(lons)))
    message(sprintf('time length: %s', length(dim_time_input)))
    message(sprintf('ncol var data: %s', ncol(var_data)))
    message(sprintf('nrow var data: %s', nrow(var_data)))
    message(sprintf('length data units: %s', length(rep(variable_metadata$units, length(gcm_cells)))))

    # write this variable to the netcdf file:
    write_timeseries_dsg(nc_file,
                         instance_names = gcm_cells,
                         lats = lats,
                         lons = lons,
                         times = dim_time_input,
                         data = var_data,
                         data_unit = rep(variable_metadata$units, length(gcm_cells)),
                         data_prec = 'double',
                         data_metadata = list(name = variable_metadata$var_name, long_name = variable_metadata$longname),
                         time_units = "days since 1970-01-01 00:00:00",
                         attributes = list('title' = global_att),
                         coordvar_long_names = list(instance = "identifier for GCM grid cell", time = "date",
                                                    lat = "latitude of GCM grid cell centroid", lon = "longitude of GCM grid cell centroid"),
                         add_to_existing = ifelse(file.exists(nc_file), TRUE, FALSE),
                         overwrite = TRUE)
  }



  #' @param alts \code{numeric} vector of altitudes (m above sea level) (Optional)
  #' @param attributes list An optional list of attributes that will be added at the global level.
  #' See details for useful attributes.
  #' @param time_units \code{character} units string in udunits format to use for time. Defaults to 'days since 1970-01-01 00:00:00'
  #' @param coordvar_long_names \code{list} values for long names on coordinate variables. Names should be `instance`, time`, `lat`, `lon`, and `alt.`
  #' @param add_to_existing \code{boolean} If TRUE and the file already exists,
  #' variables will be added to the existing file. See details for more.
  #' @param overwrite boolean error if file exists.

  # TODO: remove this step which just creates an empty file
  # with a .nc extension as a placeholder
  # writeLines("PLACEHOLDER", nc_file)

  return(nc_file)
}

