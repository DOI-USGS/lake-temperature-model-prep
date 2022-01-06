
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
                             xmin = grid_params$xmin,
                             ymin = grid_params$ymin,
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
#' polygons. The GCM grid is 110 cells wide by 85 high. With a `tile_dim` of
#' 10 (tiles = 10 grid cells x 10 grid cells), tiles won't cover full height
#' of the GCM grid and there will be 5 rows left out at the top. Since the top
#' 5 rows are fully outside CONUS, we are OK with dropping for now. If we wanted
#' to include, we would need to construct two separate `sf` grids and merge.
construct_grid_tiles <- function(grid_params, tile_dim) {
  # determine the number of columns and rows of tiles
  xcolumns <- floor(grid_params$nx/tile_dim)
  yrows <- floor(grid_params$ny/tile_dim)

  gcm_tiles <- construct_grid(cellsize = grid_params$cellsize*tile_dim,
                              nx = xcolumns,
                              ny = yrows,
                              xmin = grid_params$xmin,
                              ymin = grid_params$ymin,
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
#' @param xmin x dimension for the bottomleft corner of the grid
#' @param ymin y dimension for the bottomleft corner of the grid
#' @param crs character string representing the projection of the grid
construct_grid <- function(cellsize, nx, ny, xmin, ymin, crs) {

  # Build grid
  grid_sfc <- sf::st_make_grid(cellsize = cellsize,
                               n = c(nx, ny),
                               offset = c(xmin, ymin),
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

# TODO: scale up. For now, this just subsets the lakes
# to 5 in WI. Will want to scale up to all of MN and then
# try for every lake in centroid_lakes_sf Once we scale
# up completely, we shouldn't need this function.
subset_lake_centroids <- function(centroids_sf) {

  set.seed(19) # Same subset of 5 every time
  wi_sf <- st_as_sf(maps::map('state', 'wisconsin', plot=FALSE, fill=TRUE))

  centroids_sf %>%
    st_intersection(wi_sf) %>%
    sample_n(5)
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

#' @title Match lakes to the grid cells.
#' @description Using lake centroids, this spatially
#' intersects the lakes to the grid cells and returns a
#' non-spatial data.frame with `cell_no` and any of the existing
#' columns from `lake_centroids`.
#' @param lake_centroids an `sf` object of points representing
#' the lake centroids.
#' @param grid_cells an `sf` object with square polygons representing the
#' grid cells. Must contain a `cell_no` column which has
#' the id of each of the cell polygons.
get_lake_cell_xwalk <- function(lake_centroids, grid_cells) {
  lake_cells_join <- lake_centroids %>%
    st_join(grid_cells, left=FALSE) %>%
    st_drop_geometry()

  return(lake_cells_join)
}

# Map the query - grid cells, grid tiles, selected tile, cells w lakes in selected tile, lake centroids
# TODO: purely diagnostic while we develop the pipeline. Likely delete when done. If we decide to keep,
# this function to should be 1) moved to 8_viz, and 2) made so that it only rebuilds if new cells will
# must be plot.
map_query <- function(out_file_template, lake_centroids, grid_tiles, grid_cells, cells_w_lakes) {

  # get tile id (pull from first row - will be identical
  # for all rows, since branching over tiles)
  tile_id <- cells_w_lakes$tile_no[1]

  # get tile polygon for selected tile
  selected_tile <- grid_tiles %>% filter(tile_no == tile_id)

  # TODO: delete this WI-specific view
  wi_sf <- st_as_sf(maps::map('state', 'wisconsin', plot=FALSE, fill=TRUE)) %>%
    sf::st_transform(crs = sf::st_crs(grid_cells))

  # build plot
  query_plot <- ggplot() +
    geom_sf(data=wi_sf, fill=NA, color='grey70') +
    geom_sf(data=grid_cells, color='grey50', fill=NA, size=0.5) +
    geom_sf(data=grid_tiles, color='darkgoldenrod2', fill=NA) +
    geom_sf(data=selected_tile, color='firebrick4', fill=NA) +
    geom_sf(data=cells_w_lakes, color='firebrick2', fill=NA) +
    geom_sf(data=lake_centroids, color='dodgerblue2', size=0.5) +
    coord_sf() + theme_void()

  # save file
  out_file <- sprintf(out_file_template, tile_id)
  ggsave(out_file, query_plot, width=10, height=8, dpi=300)

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
#' GCM dataset has a separate URL for downloading data from these different periods.
#' Should be one of `1980_1999`, `2040_2059`, or `2080_2099`.
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

  # Reproject query cell centroids to WGS84
  query_geom_WGS84 <- sf::st_transform(query_geom, crs = 4326)

  # convert grid cell centroids into geoknife-friendly format
  query_simplegeom <- sf_pts_to_simplegeom(query_geom_WGS84)

  # Build query_url
  query_url <- sprintf("https://cida.usgs.gov/thredds/dodsC/notaro_%s_%s",
                       gcm_name, gcm_projection_period)

  # Retry the GDP query multiple times if it doesn't work.
  retry({
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
    wait(gcm_job)
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
#' @description The final GCM driver data will need to be daily, but geoknife
#' returns hourly values. This step summarizes the Notaro raw data into daily
#' values and converts into appropriate units. It creates a file with the exact
#' same name, except that the "_raw" part of the `in_file` filepath is replaced
#' with "_daily".
#' @param in_file filepath to a feather file containing the hourly geoknife data
munge_notaro_to_glm <- function(in_file) {

  daily_data <- arrow::read_feather(in_file) %>%

    # Pivot to long format first to get cells as a column.
    pivot_longer(cols = -c(DateTime, variable, statistic, units),
                 names_to = "cell", values_to = "val") %>%
    mutate(cell = as.numeric(cell)) %>%

    # Now pivot wider to makes each variable a column for easy, readable munging
    # Also, removes `units` and `statistic` columns which are specific to each variable
    pivot_wider(id_cols = c("DateTime", "cell"), names_from = variable, values_from = val) %>%

    # Unit conversions to get GLM-ready variables from GDP ones
    mutate(AirTemp = tas - 273.15, # Convert GDP air temperature (tas) from Kelvin to Celcius
           Rain = pr # TODO: Convert GDP precipitation (pr) from kg/m2/s to meters/hour
    ) %>%

    # Simply rename GDP variables into GLM variables
    mutate(RelHum = qas,
           Shortwave = rsns,
           Longwave = rsns # TODO: find longwave from GDP
    ) %>%

    # Calculate GLM variables using other existing variables
    mutate(Snow = ifelse(AirTemp < 0, Rain*10, 0), # When air temp is below freezing, any precip should be considered snow
           Rain = ifelse(AirTemp < 0, 0, Rain),
           # Calculate the Windspeed from wind velocity values (westerly & southerly)
           WindSpeed = sqrt(uas^2 + vas^2)
    ) %>%

    # Create a column with just the date to use for summarizing
    mutate(date = as.Date(DateTime)) %>%

    # Keep only the columns we need
    select(time = date,
           cell,
           Shortwave,
           Longwave,
           AirTemp,
           RelHum,
           WindSpeed,
           Rain,
           Snow
    ) %>%

    # Convert from hourly to daily data
    group_by(time, cell) %>%
    # TODO: should we `na.rm = TRUE`?
    summarize(across(.cols = -WindSpeed, .fns = ~ mean(.x, na.rm = FALSE)),
              WindSpeed = mean(WindSpeed^3)^(1/3),
              n = length(time),
              .groups = "keep") %>%
    ungroup() %>%
    # This drops Jan 1, 1980
    filter(n == 24) %>% select(-n)

  # Save the daily data
  out_file <- gsub("_raw.feather", "_daily.feather", in_file)
  arrow::write_feather(daily_data, out_file)

  return(out_file)
}
