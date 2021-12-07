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

  # Setup the NetCDF file with appropriate dimensions and attributes
  build_gcm_nc_skeleton(nc_file, dim_time_input, dim_cell_input, vars_info, global_att, overwrite = overwrite)

  # Populate the NetCDF file with data from each of the tiles.
  # Opens and then closes the `nc_file` each loop iteration.
  for(tile_fn in gcm_raw_files) {
    arrow::read_feather(tile_fn) %>%
      push_gcm_nc_data(nc_file, var_names = vars_info$var_name)
  }

  return(nc_file)
}


#' @title Creates a blank NetCDF file for storing GCM driver data
#' @description Creates a NetCDF file with appropriate dimensions and NA values
#' to be filled in with real values where available in `push_gcm_nc_data()`. This
#' function was adapted from functions shared by J. Zwart on 12/03/2021. See more at
#' https://code.usgs.gov/wma/wp/forecast-pgdl-da/-/blob/main/2_forecast_prep/src/nc_utils.R
#' @param nc_file netcdf file name output
#' @param dim_time_input vector of GCM driver data dates
#' @param dim_cell_input vector of all GCM driver grid cells (whether or not data was pulled)
#' @param vars_info variables and descriptions to store in NetCDF
#' @param global_att global attribute description for the data in this NetCDF file (e.g. notaro_ACCESS_1980_1999)
#' @param overwrite T/F if the nc file should be overwritten if it exists already
build_gcm_nc_skeleton <- function(nc_file, dim_time_input, dim_cell_input, vars_info, global_att, overwrite = TRUE) {

  ##### Set dimensions; should always be [time, cell] #####

  # Setup time dimension
  times <- as.integer(seq(0, length(dim_time_input) - 1, 1)) # days since dim_time_input[1]
  time_dim <- ncdim_def("time",
                        units = sprintf('days since %s', dim_time_input[1]),
                        longname = 'time',
                        vals = times)

  # Setup grid cell dimension
  cells_dim <- ncdim_def("gridcells",
                         units = "",
                         longname = 'GCM grid cell number from lake-temperature-model-prep',
                         vals = dim_cell_input)

  # Check that units are valid
  udunits2::ud.is.parseable(time_dim$units)
  udunits2::ud.is.parseable(cells_dim$units)

  ##### Define variables #####

  fillvalue <- NA # fill for missing data

  def_list <- list()

  n_vars <- nrow(vars_info)
  # loop through observation variables and define name, units, precision
  for(i in 1:n_vars){
    cur_var <- vars_info$var_name[i]

    def_list[[i]] <- ncvar_def(name =  vars_info$var_name[i],
                               units = vars_info$units[i],
                               dim = list(time_dim, cells_dim),
                               missval = fillvalue,
                               longname = vars_info$longname[i],
                               prec = vars_info$precision[i])
  }

  ##### Write out the file #####

  if(file.exists(nc_file)){
    if(overwrite){
      file.remove(nc_file)
      ncout <- nc_create(nc_file, def_list, force_v4 = T)
    }else{stop('cannot overwrite nc output file')}
  }else{ncout <- nc_create(nc_file, def_list, force_v4 = T)}


  ##### Add global file metadata #####
  ncatt_put(nc = ncout,
            varid = 0,
            attname = "file_description",
            attval = global_att,
            prec =  "text")

  nc_close(ncout)
  return(nc_file)

}

#' @title insert GCM data into an existing NetCDF file
#' @description Using the NetCDF file created from `build_gcm_nc_skeleton()`, this
#' function will populate the file with the actual data.
#' @param gcm_df data frame with the GCM data to insert. Assumes this data has a
#' date column, a column called `variable` containing values that can be matched to
#' `var_names`, and a column for each of the grid cell's data named by the cell number.
#' This argument is first so that the function can be piped to pass in the data.frame.
#' @param nc_file file path to the netcdf file
#' @param var_names vector of variable names to loop through
push_gcm_nc_data <- function(gcm_df, nc_file, var_names){

  # Open the NetCDF file
  ncout <- nc_open(nc_file, write = T)

  # Cycle through each of the variables (outer `for` loop) & then the grid cells
  # containing data (inner `for` loop). For each, extract a vector of containing
  # data for the current cell and variable and then insert into the appropriate
  # dimensions within the NetCDF file.
  for(cur_var in var_names){

    # Extract the current variable's info from the nc data
    cur_var_att <- ncout$var[[cur_var]]
    varsize <- cur_var_att$varsize
    n_dims <- cur_var_att$ndims

    # Order of the GCM dimensions as created in `build_gcm_nc_skeleton()`; [time_dim, cells_dim]
    dim_time_pos <- 1
    dim_cells_pos <- 2

    # Extract GCM values for the current variable from the bigger GCM data frame
    cur_var_df <- dplyr::filter(gcm_df, variable == cur_var)

    # Cycle through only cells that are in the `gcm_df`rather than all 9000+ grid cells for each variable
    # and each file. To find the cells that have data, force column names to be numeric and then only keep
    # ones that succeed (so this assumes that the only numeric column names are the grid cell columns, which
    # seems reasonable). As of 12/7, non-cell colnames are "date", "variable", "statistic", and "units"
    available_cells <- na.omit(as.numeric(names(gcm_df)))

    for(cur_cell in available_cells){

      # Define where to start & how many values to add for each dimension [time_dim, cells_dim]
      start <- c(1, cur_cell)
      count <- c(varsize[dim_time_pos], 1) # Use all of the slots for the time dimension, but only one for the cell dimension

      # Extract GCM values for the current cell from the corresponding column in the GCM data frame
      cur_cell_vals <- cur_var_df[[as.character(cur_cell)]]

      ncvar_put(nc = ncout,
                varid = cur_var,
                vals = cur_cell_vals,
                start = start,
                count = count)
    }
  }

  nc_close(ncout)
}


