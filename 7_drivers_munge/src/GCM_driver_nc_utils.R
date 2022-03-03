# Munging driver data from geoknife to create NetCDF for each GCM.

#' @title Creates a NetCDF file with GCM driver data
#' @description For each of the GCMs, create a single NetCDF file containing all the
#' variables and all the grid cells.
#' @param nc_file netcdf outfile name
#' @param gcm_raw_files vector of feather file paths, one for each tile with data, for the current GCM.
#' @param vars_info variables and descriptions to store in NetCDF
#' @param grid_params grid cell parameters to add to NetCDF as global attributes
#' @param spatial_info cell_nos, x indices, y indices, and WGS84 coordinates of grid cell centroids
#' @param global_att global attribute description for the observations (e.g., notaro_ACCESS_1980_1999)
#' @param compression T/F if the nc file should be compressed after creation
generate_gcm_nc <- function(nc_file, gcm_raw_files, vars_info, grid_params, spatial_info,
                            global_att, compression) {
  # NOTE: adding a stop() for now while compression code and documentation still
  # needs to be refined further, but retaining draft code below
  if (compression == TRUE) {
    stop(paste('Compression is not fully supported at this time',
               'Please re-run with the compression parameter set to FALSE',
               sep='\n'))
  }

  # Delete nc outfile if it exists already
  if (file.exists(nc_file)) {
    unlink(nc_file)
  }

  # If compressing netCDFs, create temporary nc file, and delete if exists already
  # The uncompressed netCDF will be saved to this temporary file rather than the outfile
  if (compression == TRUE) {
    compressed_outfile <- nc_file
    nc_file <- str_replace(nc_file, pattern = '.nc', '_uncompressed.nc')
    if (file.exists(nc_file)) unlink(nc_file)
  }

  # Read in all data for the current (tar_group) GCM
  gcm_data <- purrr::map_df(gcm_raw_files, function(gcm_raw_file) {
    arrow::read_feather(gcm_raw_file)
  }) %>% arrange(cell_no)

  # Pull vector of unique dates and convert to POSIXct
  gcm_dates <- as.POSIXct(unique(gcm_data$time), tz='GMT')

  # Pull vector of unique cell numbers
  gcm_cells <- unique(gcm_data$cell_no)
  gcm_cells_dim_name <- "gcm_cell_id"

  # Get WGS84 latitude and longitude of cell centroids
  cell_coords <- spatial_info %>%
    mutate(lon = sf::st_coordinates(.)[,1], lat = sf::st_coordinates(.)[,2]) %>%
    sf::st_set_geometry(NULL) %>%
    arrange(cell_no) %>%
    filter(cell_no %in% gcm_cells)

  # Get vectors of cell centroid latitudes and longitudes
  cell_centroid_lats <- cell_coords %>% pull(lat)
  cell_centroid_lons <- cell_coords %>% pull(lon)

  # Set up attributes for NetCDF that are independent of variables
  data_time_units <- "days since 1970-01-01 00:00:00"
  data_attributes <- list('title' = global_att)
  data_attributes <- append(data_attributes, grid_params) # add grid parameters to list of global attributes
  data_coordvar_long_names <- list(instance = "identifier for reconstructed Notaro downscaled GCM grid cell", time = "date",
                              lat = "WGS84 latitude of downscaled grid cell centroid",
                              lon = "WGS84 longitude of downscaled grid cell centroid")

  # Pivot data to long format to set up for filtering by variable
  gcm_data_long <- gcm_data %>%
    pivot_longer(cols = -c(time, cell_no), names_to = "variable", values_to = "value") %>%
    arrange(cell_no)

  # Loop over data variables to populate NetCDF using ncdfgeom::write_timeseries_dsg()
  for (variable in vars_info$var_name) {
    # Filter long-format data to selected variable
    variable_metadata <- vars_info %>% filter(var_name == variable)
    var_data <- gcm_data_long %>%
      filter(variable == !!variable) %>%
      pivot_wider(id_cols = c("time"), names_from = cell_no, values_from = value) %>%
      select(-time) %>%
      as.data.frame() %>%
      setNames(gcm_cells)

    # Pull attributes for selected variable
    var_data_unit <- rep(variable_metadata$units, length(gcm_cells))
    var_data_prec <- variable_metadata$data_precision
    var_data_metadata <- list(name = variable_metadata$var_name, long_name = variable_metadata$longname)

    # Check if nc file already exists, and therefore should be added to,
    # or if it needs to be created
    var_add_to_existing <- ifelse(file.exists(nc_file), TRUE, FALSE)

    # write this variable to the netcdf file:
    # ncdfgeom::write_timeseries_dsg() function documentation:
    # https://github.com/USGS-R/ncdfgeom/blob/master/R/write_timeseries_dsg.R
    ncdfgeom::write_timeseries_dsg(nc_file,
                         instance_names = gcm_cells,
                         lats = cell_centroid_lats,
                         lons = cell_centroid_lons,
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
                         overwrite = TRUE)
  }

  # If compressing netCDFs, compress the temporary netCDF file and save
  # to the final output file
  if (compression == TRUE) {
      # Note that the nc_file filename used above represents the temporary,
      # uncompressed nc file
      temp_nc_file <- nc_file
      # Redefine nc_file to be the original nc_file filename parameter,
      # stored above as `compressed_outfile`
      nc_file <- compressed_outfile

      # Run these ncdf commands from the directory of the files:
      project_dir <- setwd(dirname(nc_file))

      # Set up precision arguments for each variable using vars_info tibble
      # --ppc key1=val1#key2=val2
      precision_args <- paste(paste(vars_info$var_name, vars_info$compression_precision, sep = '='), collapse = '#')

      # Compress and quantize the file
      # This command requires that NCO be installed and able to be
      # called by R via system commands
      # see http://nco.sourceforge.net/
      system(sprintf("ncks -h --fl_fmt=netcdf4 --cnk_plc=g3d --cnk_dmn time,10 --ppc %s %s %s",
                     precision_args, basename(temp_nc_file), basename(nc_file)))
      # Switch back to the project directory
      setwd(project_dir)

      # Delete the temporary (uncompressed) file if the final compressed
      # file has been created. If it hasn't, throw an error.
      # Using this approach in place of tryCatch, since if NCO is the issue,
      # R will throw a system error message, *not* a console error
      if (file.exists(nc_file)) {
        unlink(temp_nc_file)
      } else {
        stop(paste(sprintf('The %s netCDF file could not be compressed',temp_nc_file),
                   'Make sure you have NCO netCDF operators installed on your system',
                   sep='\n'))
      }
  }
  return(nc_file)
}
