# Reconstruct GCM grid using hard-coded grid parameters
reconstruct_gcm_grid <- function(grid_params) {
  # Build GCM grid
  gcm_grid <- construct_grid(cellsize = grid_params$grid_size_m,
                             nx = grid_params$n_cell_x,
                             ny = grid_params$n_cell_y,
                             xmin = grid_params$x_min,
                             ymin = grid_params$y_min,
                             proj_str = grid_params$crs) %>%
    rename(cell_no = id)

  return(gcm_grid)
}

# Construct grid tiles using hard-coded grid parameters
# NOTE - sf_make_grid() can only  make square grid polygons
# We're using tile_dim of 10 for now (tiles = 10 grid cells x 10 grid cells)
# The grid is 110 cells wide by 85 high, and in this function we
# get the number of rows/columns by floor(n_cells[high/wide]/tile_dim) so are rounding down.
# With a tile_dim of 10, tiles won't cover full height of grid
# but top 5 rows are fully outside CONUS, so we are dropping for now
# rather than constructing two separate sf grids and merging
# works b/c st_make_grid starts in lower left corner of grid
construct_grid_tiles <- function(grid_params, tile_dim) {
  # determine the number of columns and rows of tiles
  xcolumns <- floor(grid_params$n_cell_x/tile_dim)
  yrows <- floor(grid_params$n_cell_y/tile_dim)

  gcm_tiles <- construct_grid(cellsize = grid_params$grid_size_m*tile_dim,
                              nx = xcolumns,
                              ny = yrows,
                              xmin = grid_params$x_min,
                              ymin = grid_params$y_min,
                              proj_str = grid_params$crs) %>%
    rename(tile_no = id)

  return(gcm_tiles)
}

# Shared function used to generate a grid based on grid cell size, dimensions,
# placement, and projection to use. This is used by both `reconstruct_gcm_grid()`
# and `construct_grid_tiles()` to generate grids with their appropriate configurations.
construct_grid <- function(cellsize, nx, ny, xmin, ymin, proj_str) {

  # Build grid
  grid_sfc <- sf::st_make_grid(cellsize = cellsize,
                               n = c(nx, ny),
                               offset = c(xmin, ymin),
                               crs = proj_str)

  # set up attributes for cell number, x and y grid values
  # cells count left to right, then next row, then left to right
  cell_nos <- seq(1:(nx*ny))
  x_cells <- rep(1:nx, ny)
  y_cells <- c(sapply(1:ny, function(x) rep(x, nx)))

  # construct sf dataframe
  grid_sf <- st_sf(data.frame(x = x_cells, y = y_cells, id = cell_nos), geometry=grid_sfc)

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

# Subset grid cells to those within each tile
# (targets is branching over the tile polygons)
# using grid cell centroids for clean intersection
get_tile_cells <- function(grid_cell_centroids, grid_cells, grid_tile) {
  # Figure out which cell centroids fall within each tile
  tile_cells <- grid_cell_centroids %>%
    st_intersection(grid_tile)

  # Pull the cell ids associated with each tile
  tile_cell_ids <- tile_cells$cell_no

  # Filter grid cells to those with the specified ids
  grid_cells %>%
    filter(cell_no %in% tile_cell_ids) %>%
    mutate(tile_no = grid_tile$tile_no)
}

# Filter cells associated with given tile
# to only those cells that contain lakes
get_query_cells <- function(tile_cells, lake_centroids) {
  cell_lakes_intersect <- st_intersects(tile_cells, lake_centroids)

  cells_w_lakes <- tile_cells %>% mutate(contains_lake = lengths(cell_lakes_intersect) > 0, n_lakes = lengths(cell_lakes_intersect)) %>%
    dplyr::filter(contains_lake)

  return(cells_w_lakes)

}

# Filter cell centroids to those that fall within
# the grid cells that contain lakes (for a given tile)
get_query_centroids <- function(query_cells, cell_centroids) {
  # remove cell_no column from cell_centroids, since don't need it
  # and would be duplicated on join
  cell_centroids <- cell_centroids %>% select(-cell_no)

  # keep only cell centroids for cells with lakes
  cell_centroids_w_lakes <- cell_centroids %>%
    st_join(query_cells, left=FALSE)

  return(cell_centroids_w_lakes)
}

# Get lake cell xwalk, w/o spatial information
get_lake_cell_xwalk <- function(lake_centroids, tile_cells) {
  lake_cells_join <- lake_centroids %>%
    st_join(tile_cells, left=FALSE) %>%
    st_set_geometry(NULL)

  return(lake_cells_join)
}

# Map the query - grid cells, grid tiles, selected tile, cells w lakes in selected tile, lake centroids
# For now, generating empty file for tiles that don't contain cells that contain lakes
map_query <- function(out_file_template, lake_centroids, grid_tiles, grid_cells, cells_w_lakes) {
  if (nrow(cells_w_lakes) > 0) {
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
    out_file <- gsub('XX', tile_id, out_file_template)
    ggsave(out_file, query_plot, width=10, height=8, dpi=300)

    return(out_file)
  } else {
    # MUST BE A BETTER WORKAROUND?
    # Build placeholder out_file name
    out_file <- gsub('XX', 'NULL', out_file_template)

    # write empty file
    query_plot_empty <- ggplot()
    ggsave(out_file, query_plot_empty)

    return(out_file)
  }
}

# Convert an sf object into a geoknife::simplegeom, so that
# it can be used in the geoknife query. `geoknife` only works
# with `sp` objects but not SpatialPoints at the moment, so
# need to convert these to a data.frame.
sf_pts_to_simplegeom <- function(sf_obj) {
    sf_obj %>%
      st_coordinates() %>%
      t() %>% as.data.frame() %>%
      simplegeom()
}

# Set up a download file to get the raw GCM data down.
# This function accepts each of the query parameters for the
# geoknife job as an argument. Currently missing the "knife" parameter.
download_gcm_data <- function(out_file_template, query_geom, query_url_template, gcm_name, query_vars,
                              query_dates, query_knife = NULL) {
  #IF TILE CONTAINS CELLS THAT CONTAIN LAKES
  if (nrow(query_geom) > 0) {

    # Reproject query cell centroids to WGS84
    query_geom_WGS84 <- sf::st_transform(query_geom, crs = 4326)

    # convert grid cell centroids into geoknife-friendly format
    query_simplegeom <- sf_pts_to_simplegeom(query_geom_WGS84)

    # Build query_url
    query_url <- gsub('GCMname', gcm_name, query_url_template)

    # construct and submit query
    gcm_job <- geoknife(
      stencil = query_simplegeom,
      fabric = webdata(
        url = query_url,
        variables = query_vars,
        times = query_dates
      )
    )
    wait(gcm_job)
    my_data <- result(gcm_job)

    # Build out_file name
    out_file <- gsub('GCMname', gcm_name, out_file_template)
    out_file <- gsub('XX', query_geom$tile_no[1], out_file)

    # write file
    arrow::write_feather(my_data, out_file)

    return(out_file)
  } else {
    # MUST BE A BETTER WORKAROUND?
    # Build placeholder out_file name
    out_file <- gsub('GCMname', 'NULL', out_file_template)
    out_file <- gsub('XX', 'NULL', out_file)

    # write empty data frame
    arrow::write_feather(data.frame(), out_file)

    return(out_file)
  }
}
