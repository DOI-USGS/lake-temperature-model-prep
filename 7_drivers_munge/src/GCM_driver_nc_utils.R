# Munging driver data from geoknife to create NetCDF for each GCM.

#' @title Creates a NetCDF file with GCM driver data
#' @description For each of the GCMs, create a single NetCDF file containing all the
#' variables and all the grid cells.
#' @param nc_file netcdf outfile name
#' @param gcm_raw_files vector of feather file paths, one for each tile with data for the current GCM.
#' @param vars_info variables and descriptions to store in NetCDF
#' @param grid_info projection information to add to NetCDF as grid mapping
#' @param grid_params grid cell parameters to add to NetCDF as global attributes
#' @param spatial_info cell_nos, x indices, y indices, and projected coordinates of grid cell centroids
#' @param global_att global attribute description for the observations (e.g. notaro_ACCESS_1980_1999)
#' @param overwrite T/F if the nc file should be overwritten if it exists already
generate_gcm_nc <- function(nc_file, gcm_raw_files, vars_info, grid_info, grid_params, spatial_info,
                            global_att, overwrite = TRUE) {

  # Delete nc outfile if it exists already
  if (file.exists(nc_file)) {
    unlink(nc_file)
  }

  # Read in all data for the current (tar_group) GCM
  gcm_data <- purrr::map_df(gcm_raw_files, function(gcm_raw_file) {
    arrow::read_feather(gcm_raw_file)
  }) %>% arrange(cell)

  # Pull vector of unique dates and convert to POSIXct
  gcm_dates <- as.POSIXct(unique(gcm_data$time), tz='GMT')

  # Pull vector of unique cell numbers
  gcm_cells <- unique(gcm_data$cell)
  gcm_cells_dim_name = "gcm_cell_id"

  # Get WGS84 coordinates of cell centroids
  cell_coords <- spatial_info %>%
    mutate(lon = sf::st_coordinates(.)[,1], lat = sf::st_coordinates(.)[,2]) %>%
    rename(cell = cell_no) %>%
    sf::st_set_geometry(NULL) %>%
    arrange(cell) %>%
    filter(cell %in% gcm_cells)

  # Get vectors of cell centroid coordinates
  cell_centroid_lats <- cell_coords %>% pull(lat)
  cell_centroid_lons <- cell_coords %>% pull(lon)

  # Set up attributes for NetCDF that are independent of variables
  data_time_units <- "days since 1970-01-01 00:00:00"
  data_attributes <- list('title' = global_att)
  data_attributes <- append(data_attributes, grid_params) # add grid parameters to list of global attributes
  data_coordvar_long_names <- list(instance = "identifier for GCM grid cell", time = "date",
                              lat = "Latitude of GCM grid cell centroid", lon = "Longitude of GCM grid cell centroid",
                              alt = "NULL")

  # Pivot data to long format to set up for filtering by variable
  gcm_data_long <- gcm_data %>%
    pivot_longer(cols = -c(time, cell), names_to = "variable", values_to = "value") %>%
    arrange(cell)

  # Loop over data variables to populate NetCDF using ncdfgeom::write_timeseries_dsg()
  for (variable in vars_info$var_name) {
    # Filter long-format data to selected variable
    variable_metadata <- vars_info %>% filter(var_name == variable)
    var_data <- gcm_data_long %>%
      filter(variable == !!variable) %>%
      pivot_wider(id_cols = c("time"), names_from = cell, values_from = value) %>%
      select(-time) %>%
      as.data.frame() %>%
      setNames(gcm_cells)

    # Pull attributes for selected variable
    var_data_unit <- rep(variable_metadata$units, length(gcm_cells))
    var_data_prec <- variable_metadata$precision
    var_data_metadata <- list(name = variable_metadata$var_name, long_name = variable_metadata$longname)

    # Check if nc file already exists, and therefore should be added to,
    # or if needs to be created
    var_add_to_existing <- ifelse(file.exists(nc_file), TRUE, FALSE)

    # write this variable to the netcdf file:
    # ncdfgeom::write_timeseries_dsg() function documentation:
    # https://github.com/USGS-R/ncdfgeom/blob/master/R/write_timeseries_dsg.R
    ncdfgeom::write_timeseries_dsg(nc_file,
                         instance_names = gcm_cells,
                         lats = cell_centroid_lats,
                         lons = cell_centroid_lons,
                         alts = NA,
                         times = gcm_dates,
                         data = var_data,
                         data_unit = var_data_unit,
                         data_prec = var_data_prec,
                         data_metadata = var_data_metadata,
                         time_units = data_time_units,
                         instance_dim_name = gcm_cells_dim_name,
                         dsg_timeseries_id = gcm_cells_dim_name,
                         attributes = data_attributes,
                         coordvar_long_names = data_coordvar_long_names,
                         add_to_existing = var_add_to_existing,
                         overwrite = overwrite)
  }

  # # Reopen NetCDF file in order to add grid mapping
  # # and redefine coordinate variables as auxiliary coordinate variables
  # # since the coordinate data are projected cartesian coordinates
  # nc <- RNetCDF::open.nc(nc_file, write=TRUE)
  #
  # # Add grid_mapping variable as a new scalar NetCDF variable
  # RNetCDF::var.def.nc(nc, grid_info$grid_map_variable, "NC_CHAR", NA)
  # RNetCDF::att.put.nc(nc, grid_info$grid_map_variable, "grid_mapping_name", "NC_CHAR", grid_info$grid_mapping_name)
  # RNetCDF::att.put.nc(nc, grid_info$grid_map_variable, "standard_parallel", "NC_DOUBLE", unlist(grid_info$standard_parallel))
  # RNetCDF::att.put.nc(nc, grid_info$grid_map_variable, "longitude_of_central_meridian", "NC_DOUBLE", grid_info$longitude_of_central_meridian)
  # RNetCDF::att.put.nc(nc, grid_info$grid_map_variable, "latitude_of_projection_origin", "NC_DOUBLE", grid_info$latitude_of_projection_origin)
  #
  # # Add grid mapping attribute to all data variables
  # all_vars <- vars_info$var_name
  # purrr::map(all_vars, function(var) {
  #   RNetCDF::att.put.nc(nc, var, "grid_mapping", "NC_CHAR", grid_info$grid_map_variable)
  # })
  #
  # # Rename lat lon coordinate variables to reflect that they contain
  # # auxiliary coordinates, and update attributes to meet NetCDF CF conventions
  # # for projected coordinates
  # projected_coord_vars <- tibble(
  #   old_name = c('lon', 'lat'),
  #   new_name = c('jx', 'iy'),
  #   long_name = c('x-coordinate in Cartesian system', 'y-coordinate in Cartesian system'),
  #   standard_name = c('projection_x_coordinate', 'projection_y_coordinate')
  # )
  # purrr::pmap(projected_coord_vars, function(old_name, new_name, long_name, standard_name) {
  #   RNetCDF::var.rename.nc(nc, old_name, new_name)
  #   RNetCDF::att.put.nc(nc, new_name, "units", "NC_CHAR", 'm')
  #   RNetCDF::att.put.nc(nc, new_name, "long_name", "NC_CHAR", long_name)
  #   RNetCDF::att.put.nc(nc, new_name, "standard_name", "NC_CHAR", standard_name)
  #   RNetCDF::att.put.nc(nc, new_name, "grid_mapping", "NC_CHAR", grid_info$grid_map_variable)
  # })

  # # Define separate projected coordinate variables
  # # (use if preserving lon/lat variables and adding auxiliary coordinates separately)
  # projected_coord_vars <- tibble(
  #   name = c('jx', 'iy'),
  #   long_name = c('x-coordinate in Cartesian system', 'y-coordinate in Cartesian system'),
  #   standard_name = c('projection_x_coordinate', 'projection_y_coordinate'),
  #   cell_coords_column = c('proj_x_coord', 'proj_y_coord')
  # )
  #
  # purrr::pmap(projected_coord_vars, function(name, long_name, standard_name, cell_coords_column) {
  #   # define as a new netCDF variable
  #   RNetCDF::var.def.nc(nc, name, "NC_DOUBLE", gcm_cells_dim_name)
  #   # define units
  #   RNetCDF::att.put.nc(nc, name, "units", "NC_CHAR", 'm')
  #   # define long name
  #   RNetCDF::att.put.nc(nc, name, "long_name", "NC_CHAR", long_name)
  #   # define standard names
  #   RNetCDF::att.put.nc(nc, name, "standard_name", "NC_CHAR", standard_name)
  #   # add projected coordinates to netCDF
  #   RNetCDF::var.put.nc(nc, name, cell_coords[[cell_coords_column]])
  # })

  # RNetCDF::close.nc(nc)

  return(nc_file)
}
