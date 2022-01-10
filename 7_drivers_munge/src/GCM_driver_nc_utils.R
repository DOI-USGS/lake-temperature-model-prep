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

  # Delete nc outfile if it exists already
  if (file.exists(nc_file)) {
    unlink(nc_file)
  }

  # Use `ncdfgeom` to write out the NetCDF DSG file
  # See example at https://github.com/jread-usgs/lakesurf-data-release/blob/main/src/ncdf_utils.R#L66-L76
  # And more discussion in https://github.com/USGS-R/lake-temperature-model-prep/issues/252
  # ncdfgeom::write_timeseries_dsg(nc_file)

  # Read in all data for that GCM and pivot to long format
  gcm_data_long <- purrr::map_df(gcm_raw_files, function(gcm_raw_file) {
    arrow::read_feather(gcm_raw_file)
  }) %>%
    pivot_longer(cols = -c(time, cell), names_to = "variable", values_to = "value") %>%
    arrange(cell)

  # pull vector of gcm cell numbers to use as instance names
  gcm_cells <- unique(gcm_data_long$cell)

  # get cell coordinates
  cell_lon_lat <- spatial_info %>%
    mutate(lon = sf::st_coordinates(.)[,1], lat = sf::st_coordinates(.)[,2]) %>%
    rename(cell = cell_no) %>%
    sf::st_set_geometry(NULL) %>%
    arrange(cell) %>%
    filter(cell %in% gcm_cells)

  cell_lats <- cell_lon_lat %>% pull(lat)
  cell_lons <- cell_lon_lat %>% pull(lon)

  data_time_units <- "days since 1970-01-01 00:00:00"
  data_attributes <- list('title' = global_att)
  data_coordvar_long_names <- list(instance = "identifier for GCM grid cell", time = "date",
                              lat = "latitude of GCM grid cell centroid", lon = "longitude of GCM grid cell centroid",
                              alt = "NULL")

  # Loop over data variables to populate netCDF using ncdfgeom::write_timeseries_dsg()
  for (variable in vars_info$var_name) {
    variable_metadata <- vars_info %>% filter(var_name == variable)
    var_data <- gcm_data_long %>%
      filter(variable == !!variable) %>%
      pivot_wider(id_cols = c("time"), names_from = cell, values_from = value) %>%
      select(-time) %>%
      as.data.frame() %>%
      setNames(gcm_cells)

    var_data_unit <- rep(variable_metadata$units, length(gcm_cells))
    var_data_prec <- variable_metadata$precision
    var_data_metadata <- list(name = variable_metadata$var_name, long_name = variable_metadata$longname)

    var_add_to_existing <- ifelse(file.exists(nc_file), TRUE, FALSE)

    # # messages to check dims
    # message(sprintf('variable: %s', variable))
    # message(sprintf('cell length: %s', length(gcm_cells)))
    # message(sprintf('lat length: %s', length(cell_lats)))
    # message(sprintf('lon length: %s', length(cell_lons)))
    # message(sprintf('time length: %s', length(dim_time_input)))
    # message(sprintf('ncol var data: %s', ncol(var_data)))
    # message(sprintf('nrow var data: %s', nrow(var_data)))
    # message(sprintf('length data units: %s', length(var_data_unit)))

    # browser()

    # write this variable to the netcdf file:
    # ncdfgeom::write_timeseries_dsg() function documentation:
    # @param nc_file \code{character} file path to the nc file to be created.
    # @param instance_names \code{character} or \code{numeric} vector of names for each instance
    # (e.g. station or geometry) to be added to the file.
    # @param times \code{POSIXct} vector of times. Must be of type \code{POSIXct} or an attempt to
    # convert it will be made using \code{as.POSIXct(times)}.
    # @param lats \code{numeric} vector of latitudes
    # @param lons \code{numeric} vector of longitudes
    # @param data \code{data.frame} with each column corresponding to an instance. Rows correspond to
    # time steps. nrow must be the same length as times. Column names must match instance names.
    # @param alts \code{numeric} vector of altitudes (m above sea level) (Optional)
    # @param data_unit \code{character} vector of data units. Length must be the same as number
    # of columns in \code{data} parameter.
    # @param data_prec \code{character} precision of observation data in NetCDF file.
    # Valid options: 'short' 'integer' 'float' 'double' 'char'.
    # @param data_metadata \code{list} A named list of strings: list(name='ShortVarName', long_name='A Long Name')
    # @param attributes list An optional list of attributes that will be added at the global level.
    # @param time_units \code{character} units string in udunits format to use for time. Defaults to 'days since 1970-01-01 00:00:00'
    # @param coordvar_long_names \code{list} values for long names on coordinate variables. Names should be `instance`, time`, `lat`, `lon`, and `alt.`
    # @param add_to_existing \code{boolean} If TRUE and the file already exists,
    # variables will be added to the existing file. See details for more.
    # @param overwrite boolean error if file exists.
    ncdfgeom::write_timeseries_dsg(nc_file,
                         instance_names = gcm_cells,
                         lats = cell_lats,
                         lons = cell_lons,
                         alts = NA,
                         times = dim_time_input,
                         data = var_data,
                         data_unit = var_data_unit,
                         data_prec = var_data_prec,
                         data_metadata = var_data_metadata,
                         time_units = data_time_units,
                         attributes = data_attributes,
                         coordvar_long_names = data_coordvar_long_names,
                         add_to_existing = var_add_to_existing,
                         overwrite = TRUE)
  }

  return(nc_file)
}

