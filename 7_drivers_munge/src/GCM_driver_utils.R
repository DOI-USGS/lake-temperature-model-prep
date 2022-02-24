
#' @title Create an `sf` objecting with square grid cells.
#' @description Reconstruct GCM grid using hard-coded grid parameters.
#' @param grid_params list with five elements: `crs`, `cellsize`,
#' `xmin`, `ymin`, `nx`, and `ny`. See descriptions of these elements
#' in the documentation for `construct_grid()` below.
reconstruct_gcm_grid <- function(grid_params) {
  # Build GCM grid
  gcm_grid <- construct_grid(cellsize = grid_params$cellsize,
                             nx = grid_params$nx,
                             ny = grid_params$ny,
                             xoffset = grid_params$xmin - grid_params$cellsize/2,
                             yoffset = grid_params$ymin - grid_params$cellsize/2,
                             crs = grid_params$crs) %>%
    rename(cell_no = id)

  return(gcm_grid)
}

#' @title Create an `sf` objecting representing groups of square
#' grid cells that we are calling "tiles".
#' @description Construct grid tiles (groups of grid cells) using
#' hard-coded grid parameters. Grid tiles can be used to group
#' queries to the Geodata Portal into more manageable sizes.
#' @param grid_params list with five elements: `crs`, `cellsize`,
#' `xmin`, `ymin`, `nx`, and `ny`. See descriptions of these elements
#' in the documentation for `construct_grid()` below.
#' @param tile_dim single numeric value representing how many grid cells to
#' group into a single tile. NOTE - sf_make_grid() can only make square grid
#' polygons. The GCM grid is 217 cells wide by 141 high. With a `tile_dim` of
#' 15 (tiles = 15 grid cells x 15 grid cells), tiles won't cover full height
#' of the GCM grid and there will be 7 columns left out on the right and 5 left
#' out on the top. Since all of the cells that are missing are fully outside CONUS,
#' we are OK with dropping for now. If we wanted to include, we would need to
#' construct two separate `sf` grids and merge.
construct_grid_tiles <- function(grid_params, tile_dim) {
  # determine the number of columns and rows of tiles
  xcolumns <- floor(grid_params$nx/tile_dim)
  yrows <- floor(grid_params$ny/tile_dim)

  gcm_tiles <- construct_grid(cellsize = grid_params$cellsize*tile_dim,
                              nx = xcolumns,
                              ny = yrows,
                              # Tile bottomleft should start at bottomleft cell
                              # corner, but xmin/ymin is the cell centroid
                              xoffset = grid_params$xmin - grid_params$cellsize/2,
                              yoffset = grid_params$ymin - grid_params$cellsize/2,
                              crs = grid_params$crs) %>%
    rename(tile_no = id)

  return(gcm_tiles)
}

#' @title Create an `sf` object representing a square grid
#' @description Shared function used to generate a grid based on grid cell size, dimensions,
#' placement, and projection to use. This is used by both `reconstruct_gcm_grid()`
#' and `construct_grid_tiles()` to generate grids with their appropriate configurations.
#' @param cellsize numeric value representing the dimensions of the square grid cell in meters.
#' @param nx number of cells to place in the x direction
#' @param ny number of cells to place in the y direction
#' @param xoffset x dimension for the bottomleft corner of the grid
#' @param yoffset y dimension for the bottomleft corner of the grid
#' @param crs character string representing the projection of the grid
construct_grid <- function(cellsize, nx, ny, xoffset, yoffset, crs) {

  # Build grid
  grid_sfc <- sf::st_make_grid(cellsize = cellsize,
                               n = c(nx, ny),
                               # Use the bottomleft corner of the bottomleft
                               # cell and not its centroid.
                               offset = c(xoffset, yoffset),
                               crs = crs)

  # set up attributes for cell number, x and y grid values
  # cells count left to right, then next row, then left to right
  cell_nums <- seq(1:(nx*ny))
  xcells <- rep(1:nx, ny)
  ycells <- c(sapply(1:ny, function(x) rep(x, nx)))

  # construct sf dataframe
  grid_sf <- st_sf(data.frame(x = xcells, y = ycells, id = cell_nums), geometry=grid_sfc)

  return(grid_sf)
}

#' @title Match grid cells to the grid tiles.
#' @description Using grid cell centroids, this spatially
#' intersects the cells to the bigger tiles and returns a
#' non-spatial data.frame with `cell_no` and `tile_no`
#' columns to map each grid cell to the tile they are inside.
#' @param grid_cell_centroids an `sf` object of points representing
#' the grid cell centroids. Must contain a `cell_no` column which has
#' the id of each of the cell centroids
#' @param grid_tiles an `sf` object with polygons representing the
#' groups of grid cells. Must contain a `tile_no` column which has
#' the id of each of the tile polygons.
get_cell_tile_xwalk <- function(grid_cell_centroids, grid_tiles) {
  # Figure out which cell centroids fall within each tile
  # Don't need to keep geometry
  grid_cell_centroids %>%
    st_intersection(grid_tiles) %>%
    st_drop_geometry() %>%
    select(cell_no, tile_no)
}

#' @title Filter grid cells to only those cells that contain lakes
#' @description Using an sf object of the grid cells and an sf object
#' of the lake centroids, this returns a data.frame with the number
#' of lakes contained in each cell, `n_lakes` and the `cell_no` for
#' any cell with at least one lake inside.
#' @param grid_cells an `sf` object of the square grid polygons. Assumes
#' that this is already in the same projection as `lake_centroids`. Must
#' contain a column called `cell_no`.
#' @param lake_centroids an `sf` object of points representing the
#' centroid for each lake. Assumes that this is already in the same
#' projection as `grid_cells`.
get_query_cells <- function(grid_cells, lake_centroids) {
  # Intersect the lake centroids with the grid cells.
  cell_lakes_intersect <- st_intersects(grid_cells, lake_centroids)

  # Keep only cells where there was at least one lake
  cells_w_lakes <- grid_cells %>%
    # Add column to say how many lakes the cell contains
    dplyr::mutate(n_lakes = lengths(cell_lakes_intersect)) %>%
    # Reorder the new column so that it is right after cell_no
    dplyr::relocate(n_lakes, .after = cell_no) %>%
    # Then, filter only to those containing at least one lake
    dplyr::filter(n_lakes > 0) %>%
    # Don't include geometry
    st_drop_geometry()

  return(cells_w_lakes)
}

#' @title Spatially match lakes to the grid cells and tiles.
#' @description Using lake centroids, this spatially intersects the lakes to
#' the grid cells and returns a non-spatial data.frame with `site_id`,
#' `state`, `cell_no` and `tile_no`, as the function also joins the
#' cell_tile xwalk in order to add the tile number for each lake.
#' @param lake_centroids an `sf` object of points representing the lake
#' centroids.
#' @param grid_cells an `sf` object with square polygons representing the
#' grid cells. Must contain a `cell_no` column which has
#' the id of each of the cell polygons.
#' @param cell_tile_xwalk mapping of which cells are in which tiles
#' @return An output table with the fields `site_id`, `state`, `cell_no` and
#' `tile_no`
get_lake_cell_tile_spatial_xwalk <- function(lake_centroids, grid_cells, cell_tile_xwalk) {
  lake_cells_tiles_xwalk <- lake_centroids %>%
    st_join(grid_cells, left=FALSE) %>%
    st_drop_geometry() %>%
    left_join(cell_tile_xwalk, by='cell_no') %>%
    select(site_id, state, cell_no, tile_no)

  return(lake_cells_tiles_xwalk)
}

#' @title Adjust the lake-cell-tile xwalk by matching lakes to the queried cells
#' that returned data.
#' @description Using lake centroids, this spatially matches lakes to query
#' cells that returned data, based on which cell centroid is closest to each
#' lake centroid. The output of this function will only differ from the output
#' of `get_lake_cell_tile_spatial_xwalk()` for those lakes that fell within
#' cells that did not return data
#' @param spatial_xwalk mapping of which lakes are in which cells and tiles,
#' based on a spatial join
#' @param lake_centroids an `sf` object of points representing the lake
#' centroids.
#' @param query_cell_centroids  an `sf` object of points representing the
#' grid cell centroids for queried cells. Must contain a `cell_no` column
#' which has the id of each of the cell centroids
#' @param cell_info a table with one row per query cell-gcm combo, with columns for
#' `gcm`, `tile_no`, `cell_no` and a column for `missing_data` indicating whether
#' or not the cell is missing data for any variable for that gcm. This table is
#' used here to filter the grid cell centroids to only those that returned data
#' @return An output table with the fields `site_id`, `state`, `cell_no` and
#' `tile_no`
adjust_lake_cell_tile_xwalk <- function(spatial_xwalk, lake_centroids, query_cell_centroids, cell_info) {
  # Pivot the cell_info tibble wider so that we have a single row per cell
  # and can track for how many gcms each cell is or is not missing data
  cell_status <- cell_info %>%
    pivot_wider(names_from='gcm', values_from='missing_data', names_glue="{gcm}_missing_data") %>%
    mutate(n_gcm_missing_data = rowSums(across(c(-cell_no,-tile_no))))

  # determine which cells returned data for all 6 GCMs
  cells_with_data <- cell_status %>%
    filter(n_gcm_missing_data == 0) %>%
    pull(cell_no)

  # filter the cell centroids to only those cells with data
  cell_centroids_with_data <- query_cell_centroids %>%
    filter(cell_no %in% cells_with_data)

  # match each lake to a cell that returned data based on
  # which cell centroid is closest to each lake centroid
  lake_cells_with_data_tiles_xwalk <- lake_centroids %>%
    st_join(cell_centroids_with_data, join=st_nearest_feature, left=TRUE) %>%
    st_drop_geometry() %>%
    select(site_id, state, cell_no, tile_no)

  # join this adjusted xwalk to the spatial xwalk
  adjusted_xwalk <- spatial_xwalk %>%
    left_join(lake_cells_with_data_tiles_xwalk,
              by=c('site_id', 'state'),
              suffix=c('_spatial','_data'))

  return(adjusted_xwalk)
}

#' @title Map the tiles and cells
#' @description Map the grid cells w/ lakes (symbolized by n_lakes per cell) and grid tiles
#' from the provided lake_cell_tile_xwalk
#' @param out_file name of output png file
#' @param lake_cell_tile_xwalk mapping of which lakes are in which cells and tiles
#' @param grid_tiles an`sf` object with polygons representing the
#' groups of grid cells. Must contain a `tile_no` column which has
#' the id of each of the tile polygons.
#' @param grid_cells an `sf` object with square polygons representing the
#' grid cells. Must contain a `cell_no` column which has
#' the id of each of the cell polygons.
#' @return a png of the xwalk grid cells, symbolized by n_lakes per cell, and the grid tiles
# TODO: This function to should be 1) moved to 8_viz, and 2) made so that it only rebuilds if
# new cells will be plotted.
map_tiles_cells <- function(out_file, lake_cell_tile_xwalk, grid_tiles, grid_cells) {
  lakes_per_cell <- lake_cell_tile_xwalk %>%
    group_by(cell_no) %>%
    summarize(nlakes = n())

  grid_tiles <- grid_tiles %>%
    filter(tile_no %in% lake_cell_tile_xwalk$tile_no)

  tile_labels <- grid_tiles %>%
    mutate(bbox=split(.,tile_no) %>% purrr::map(sf::st_bbox)) %>%
    unnest_wider(bbox)

  grid_cells <- grid_cells %>%
    filter(cell_no %in% lake_cell_tile_xwalk$cell_no) %>%
    left_join(lakes_per_cell)

  tile_cell_plot <- ggplot() +
    geom_sf(data = grid_cells, aes(fill = nlakes)) +
    scico::scale_fill_scico(palette = "batlow", direction = -1) +
    geom_sf(data = grid_tiles, fill = NA, size = 2) +
    geom_text(data = tile_labels, aes(label=tile_no,x=xmin, y=ymax), size=8, nudge_x = 15000, nudge_y = -30000, hjust = 0) +
    theme(axis.title.y=element_blank(),
        axis.title.x=element_blank())

  # save file
  ggsave(out_file, tile_cell_plot, width=10, height=8, dpi=300)

  return(out_file)
}

#' @title Convert an sf object into a geoknife::simplegeom, so that
#' it can be used in the geoknife query.
#' @description `geoknife` only works  with `sp` objects but not `sf`
#' objects, so you need to convert these to a data.frame.
#' @param sf_obj spatial features object with at least a `geometry`
#' field and a `cell_no` field. `cell_no` contains the names for each
#' of the polygons used in the query.
sf_pts_to_simplegeom <- function(sf_obj) {
  sf_obj %>%
    st_coordinates() %>%
    t() %>% as.data.frame() %>%
    # Include the cell numbers in the GDP geom so that the
    # results can be linked to the correct cells
    setNames(sf_obj$cell_no) %>%
    simplegeom()
}

#' @title Download GCM data from GDP for each tile and save to a feather file.
#' @description This function accepts each of the query parameters for the
#' geoknife job as an argument. Currently missing the "knife" parameter.
#' The result of the geoknife job is saved as a feather file.
#' @param out_file_template string representing the filepath at which to save
#' the feather file output of the data returned from GDP. The first `%s` is
#' used as the placeholder for the `gcm_name` and the second is for the `tile_no`.
#' @param query_geom an `sf` object that represents the polygons to use to query
#' GDP. Will reproject to EPS = 4326 to query GDP. Expects a column called
#' `tile_no` to represent the current group of grid cells being queried.
#' @param gcm_name name of one of the six GCMs to use to construct the query URL.
#' @param gcm_projection_period name given to the different projection periods. Each
#' GCM dataset contains data for all of the available projection time periods.
#' @param query_vars character vector of the variables that should be downloaded
#' from each of the GCMs. For a list of what variables are available, see
#' https://cida.usgs.gov/thredds/ncss/notaro_GFDL_2040_2059/dataset.html.
#' Note that we can't use `mrso` until https://github.com/USGS-R/geoknife/issues/399
#' is fixed, but we shouldn't need it for the GLM runs that these data feed anyways.
#' @param query_dates character or date vector with two dates: the first is the
#' earliest date in your query and the second is the latest date in your query.
#' @param query_knife the algorithm used to summarize the ouput. Currently `NULL`,
#' which uses `geoknife` defaults.
download_gcm_data <- function(out_file_template, query_geom,
                              gcm_name, gcm_projection_period, query_vars,
                              query_dates, query_knife = NULL) {

  # convert grid cell centroids into geoknife-friendly format
  query_simplegeom <- sf_pts_to_simplegeom(query_geom)

  # Build query_url
  query_url <- sprintf(
    "http://gdp-netcdfdev.cr.usgs.gov:8080/thredds/dodsC/notaro_debias_%s",
    tolower(gcm_name))

  # Stop now if you aren't on VPN
  if(!RCurl::url.exists(paste0(query_url, ".html")))
    stop("You must be connected to USGS VPN to download debiased GCMs")

  # Retry the GDP query multiple times if it doesn't work.
  retry({

    # TODO: temporary fix to be able to download data. Change
    # after https://github.com/USGS-R/geoknife/issues/366 resolved.
    gconfig('sleep.time' = 45, 'wait' = TRUE)

    # construct and submit query
    gcm_job <- geoknife(
      stencil = query_simplegeom,
      fabric = webdata(
        url = query_url,
        variables = query_vars,
        times = query_dates
      )
      # The default knife algorithm is appropriate
    )

    gcm_job_status <- check(gcm_job)$statusType
  },
  # Check the value of the last thing in the expr above (denoted
  # by the ".") to decide if you should retry or not
  until = ~ . == "ProcessSucceeded",
  max_tries = 5)

  my_data <- result(gcm_job, with.units = TRUE)

  # Build out_file name
  out_file <- sprintf(out_file_template,
                      gcm_name,
                      gcm_projection_period,
                      query_geom$tile_no[1])

  # write file
  arrow::write_feather(my_data, out_file)

  return(out_file)
}

#' @title Pull out individual file information from a dynamically mapped target
#' @description Use a dynamically mapped target to extract information about
#' the individual branch files to build a table that can group files and then
#' trigger rebuilds when individual files within a group change.
#' @param dynamic_branch_names character vector of the names of each branch created
#' from running `names()` using the target dynamically branched target as input.
#' @return a tibble with three columns (`name` = target name of the branch, `path`
#' = character string of the file created by that branch, and `data` = hash code
#' indicating the current state of the file) and a row for each branch.
build_branch_file_hash_table <- function(dynamic_branch_names) {
  # Get metadata for all the branches of the dynamic target
  tar_meta(all_of(dynamic_branch_names), c("path", "data")) %>%
    # There is just one path per branch, so remove the `list` format from this column
    unnest(path)
}

#' @title Convert Notaro GCM data to GLM-ready data
#' @description The final GCM driver data needs certain column names and units
#' to be used for GLM. This function converts GCM variables into appropriate
#' units and saves a file with the exact same name as the in file, except that
#' the "_raw" part of the `in_file` filepath is replaced with "_munged". Cells
#' that did not return data (contain NaN values) for *any* variable are
#' excluded from the munged data.
#' @param in_file filepath to a feather file containing the hourly geoknife
#' data, named with the the gcm, dates, and tile number as
#' '7_drivers_munge/tmp/7_GCM_{gcm_name}_{projection_period}_tile{tile_no}_raw.feather'
#' @param gcm_name name of one of the six GCMs, for which data is being read
#' @param tile_no id of the tile polygon used to group the cells for the data query
#' @value a table saved as a feather file with the following columns:
#' `time`: class Date denoting a single day
#' `cell_no`: a numeric value indicating which cell in the grid that the data
#' belongs to (note: the data table only includes queried cells that returned
#' data for all queried variables)
#' `Shortwave`: shortwave radiation (W/m2); a numeric value copied from the Notaro `rsds_debias` variable
#' `Longwave`: longwave radiation (W/m2); a numeric value copied from the Notaro `rsdl_debias` variable
#' `AirTemp`: air temperature (C); a numeric value copied from the Notaro `tas_debias` variable
#' `RelHum`: relative humidity (%); a numeric value copied from the Notaro `rh_debias` variable
#' `WindSpeed`: wind speed (m/s); a numeric value copied from the Notaro `rsdl_debias` variable
#' `Rain`: rate of precipitation as water (m/day); a numeric value converted to m/day from the
#' Notaro variable `prcp_debias` which is in mm/day.
#' `Snow`: the rate of snowfall (m/day); a numeric value derived from the `Rain`
#' column and assumes the snow depth is 10 times the water equivalent (`Rain`) when the
#' temperature (`AirTemp`) is below freezing.
#' @return a list with two elements: 1) `file_out` - the name of the output
#' feather file, and 2) `cell_info` - a tibble with a row for each cell in the
#' tile for which data are being munged, with the columns `gcm` - the gcm for which data is
#' being munged, `tile_no` the tile for which data are being munged, `cell_no` for the cell
#' number, and `missing_data` - a T/F logical indicating whether or not the cell is
#' missing data for *any* variable for the gcm for which data are being munged.
munge_notaro_to_glm <- function(in_file, gcm_name, tile_no) {

  raw_data <- arrow::read_feather(in_file)

  # This line will fail if the units don't match our assumptions
  validate_notaro_units_assumptions(raw_data)

  # Figure out if any columns contain NaNs. raw_data is in wide format, with
  # columns named by cell_no, so can pull cells that contain NaNs from column
  # names. Use 'any' as the third argument to `apply()` to catch cells where
  # any variables are missing for a cell [CURRENT APPROACH], or 'all' as the
  # third argument to catch cells where all variables are missing for a cell
  col_is_na <- apply(is.na(raw_data), 2, any)
  na_cell_colnames <- colnames(raw_data)[col_is_na]

  # Remove columns for cells that contain NaNs.
  raw_data_excl_na_cells <- select(raw_data, -all_of(na_cell_colnames))

  # Munge the raw data for all cells that returned data
  munged_data <- raw_data_excl_na_cells %>%

    # Create a column with the actual date
    convert_notaro_dates() %>%

    # Pivot to long format first to get cells as a column.
    pivot_longer(cols = -c(date, variable, statistic, units),
                 names_to = "cell_no", values_to = "val") %>%
    mutate(cell_no = as.numeric(cell_no)) %>%

    # Now pivot wider to makes each variable a column for easy, readable munging
    # Also, removes `units` and `statistic` columns which are specific to each variable
    pivot_wider(id_cols = c("date", "cell_no"), names_from = variable, values_from = val) %>%

    # Unit conversions to get GLM-ready variables from GCM ones
    mutate(
      Rain = prcp_debias / 1000 # mm to m
    ) %>%

    # Simply rename GCM variables into GLM variables
    mutate(AirTemp = tas_debias,
           RelHum = rh_debias,
           Shortwave = rsds_debias,
           Longwave = rsdl_debias,
           WindSpeed = windspeed_debias
    ) %>%

    # Calculate GLM variables using other existing variables
    mutate(Snow = ifelse(AirTemp < 0, Rain*10, 0), # When air temp is below freezing, any precip should be considered snow
           Rain = ifelse(AirTemp < 0, 0, Rain)
    ) %>%

    # Catch NA values generated through calculations for Rain and Snow variables
    # should be NaN values to be consistent with other variables and for writing to netCDF
    mutate(Snow = ifelse(is.na(Snow), NaN, Snow),
           Rain = ifelse(is.na(Rain), NaN, Rain)
    ) %>%

    # Keep only the columns we need
    select(time = date,
           cell_no,
           Shortwave,
           Longwave,
           AirTemp,
           RelHum,
           WindSpeed,
           Rain,
           Snow
    )

  # Save the munged data
  out_file <- gsub("_raw.feather", "_munged.feather", in_file)
  arrow::write_feather(munged_data, out_file)

  # Create an output tibble documenting which cells are and are not
  # missing data, noting the gcm name and tile_no
  cell_info <- bind_rows(
    tibble(cell_no = unique(munged_data$cell_no), missing_data = FALSE),
    tibble(cell_no = as.numeric(na_cell_colnames), missing_data = TRUE)) %>%
    mutate(gcm = gcm_name, tile_no = tile_no, .before=1) %>%
    arrange(cell_no)

  # return the out_file name and the cell_info tibble in a list
  return(list(file_out = out_file, cell_info=cell_info))
}

#' @title Check that units for variables downloaded match our assumptions.
#' @description Before the rest of `munge_notaro_to_glm()` can run, we need
#' to make sure that data are returned in the units that the conversion
#' functions are set up to handle. If they have different units OR if there
#' is a new variable not in our list of assumed units, then an error is thrown.
#' @param data_in a data.frame with a least the columns `variable` and `units`
validate_notaro_units_assumptions <- function(data_in) {

  # Check units assumptions
  units_check_out <- data_in %>%
    select(variable, units) %>%
    unique() %>%
    mutate(passes_assumption = case_when(
      variable == "prcp_debias" ~ units == "mm/day",
      variable == "tas_debias" ~ units == "C",
      variable == "rh_debias" ~ units == "%",
      variable == "rsds_debias" ~ units == "W/m2",
      variable == "rsdl_debias" ~ units == "W/m2",
      variable == "windspeed_debias" ~ units == "m/s",
      # For any variable not in our checks, return false
      TRUE ~ FALSE
    ))

  all_passed <- all(units_check_out$passes_assumption)

  # Cause failure if any of these units are different or there is an
  # variable in the dataset that does not appear in this list.
  if(!all_passed) {
    failed_i <- which(!units_check_out$passes_assumption)
    stop_message <- sprintf("The following units do not match the assumptions: %s",
                            paste(units_check_out$variable[failed_i], collapse = ", "))
    stop(stop_message)
  }
}

#' @title Create correct dates for downscaled Notaro GCM data
#' @description Called within `munge_notaro_to_glm()`to correctly use the
#' `DateTime` and `time(day of year)` columns to create a date. The reason this
#' is more complex than simple addition of this two fields is because 1) the
#' downscaled GCM data assumes there are no leap days, and 2) the dates returned
#' by GDP in `DateTime` do not always reflect the appropriate start day for the
#' current year of data, e.g. the `DateTime` values for 1985 data are `1984-12-31 23:15:03`.
#' @param data_in a data.frame with a least the columns `DateTime` (a POSIXct
#' object with the first day of each year of data), `time(day of year)` (a numeric
#' column with the day of year, ranging from 0:364), and `variable` (the downscaled
#' GCM column containing data variables, e.g. `prcp_debias`).
#' @value a data.frame with the columns `date`, `variable`, and any other column from
#' `data_in` that is not `DateTime` or `time(day of year)`.
convert_notaro_dates <- function(data_in) {
  data_in %>%

    ### Group data into the different big time chunks by finding and identifying ###

    # This groups data at the points where 2000 is right before 2040, and 2040 is right before 2060
    mutate(diff_days = abs(as.numeric(DateTime - lag(DateTime))/86400)) %>%
    # Use a difference in days greater than one year to determine if a row is the
    # start of a new time period
    mutate(new_time_period = is.na(diff_days) | diff_days > 366) %>%
    # Each new time period ticks the counter up by 1
    mutate(new_time_period_i = cumsum(new_time_period)) %>%

    ### For each time period + variable group, identify the earliest year in the period and use that to create a column of years ###

    # Not able to just to `format(DateTime, "%Y")` to get the year because not all have a `DateTime` in the
    # correct year, e.g. 1985 records have a DateTime of `1984-12-31 23:15:03`.

    # First treat rows for the same variable and time period as a group
    group_by(variable, new_time_period_i) %>%
    # Determine the earliest year in the time period
    mutate(earliest_year = as.numeric(format(min(DateTime), "%Y"))) %>%
    # Create groups of the years (`time(day of year)` will cycle through 0:364 for each
    # year, so tick a counter up for every 0 in the column)
    mutate(year_n = cumsum(`time(day of year)` == 0) - 1) %>%
    mutate(year_i = earliest_year + year_n) %>%
    ungroup() %>%

    ### For each year + variable group, correctly convert to the DateTime field to a date ###

    # The GCMs assume there are no leap years, but R automatically assumes leap years, so we need
    # to implement a work around for that.

    group_by(variable, new_time_period_i, year_i) %>%
    # Start by creating a date and allow leap days to be added
    mutate(date_initial = as.Date(sprintf("%s-01-01", year_i)) + `time(day of year)`) %>%
    # If the date in the column is a leap day (Feb 29), the dates need to be adjusted by 1 day
    # meaning the 59th day should be Mar 1 not Feb 29.
    mutate(leap_adjustment = if_else(format(date_initial, "%m-%d") == "02-29", 1, 0)) %>%
    # Use cumsum to make sure the leap_adjustment is used for every subsequent day of the year following a leap day
    mutate(total_days_to_add = cumsum(leap_adjustment)) %>%
    ungroup() %>% # Reset groups since they are no longer needed
    # Using the initial date created, adjust the days to shift anything in a leap year that follows Feb 29
    mutate(date_corrected = date_initial + total_days_to_add) %>%

    # Keep the variable, corrected date, statistic, and units columns, as well as, any column
    # whose name only contains numbers (since those should represent all the grid cell's data)
    select(date = date_corrected, matches("[0-9]+"), variable, statistic, units)
}
