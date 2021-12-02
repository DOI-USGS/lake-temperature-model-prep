# Pull GCM crs from GCM netcdf
pull_gcm_crs <- function(gcm_data) {
  # pull projection information
  gcm_map <- ncdf4::ncatt_get(gcm_data, 'rcm_map')
  gcm_crs <- sprintf("+proj=lcc +lat_0=%s + lon_0=%s +lat_1=%s +lat_2=%s +x_0=0 +y_0=0 +ellps=WGS84 +units=m",
                     gcm_map$latitude_of_projection_origin,
                     gcm_map$longitude_of_central_meridian,
                     gcm_map$standard_parallel[1],
                     gcm_map$standard_parallel[2])
  return(gcm_crs)
}

# Reconstruct GCM grid from a GCM netcdf
reconstruct_gcm_grid <- function(gcm_nc) {
  gcm_data <- ncdf4::nc_open(gcm_nc)

  # pull projection information
  gcm_crs <- pull_gcm_crs(gcm_data)

  # set grid size (there is a 'grid_size_in_meters' attribute,
  # but I can't seem to pull it with ncatt_get)
  grid_size_m <- 25000

  # pull projected coordinates from netcdf (note - in units of **kilometers**)
  y_coord <- ncdf4::ncvar_get(gcm_data, 'iy')
  x_coord <- ncdf4::ncvar_get(gcm_data, 'jx')

  # get number of grid cells in x and y dimensions
  y_num <- length(y_coord) - 1
  x_num <- length(x_coord) - 1

  # get offset (lower left corner of grid)
  # multiply by 1000 to get in units of meters, since crs unit is meters
  y_min <- y_coord[1]*1000
  x_min <- x_coord[1]*1000

  # Build grid
  gcm_grid_sfc <- sf::st_make_grid(cellsize = grid_size_m, n = c(x_num, y_num), offset = c(x_min, y_min), crs = gcm_crs)
  # set up attributes for cell number, x and y grid values
  # cells count left to right, then next row, then left to right
  cell_nos <- seq(1:(x_num*y_num))
  x_cells <- rep(1:(x_num), y_num)
  y_cells <- c(sapply(1:(y_num), function(x) rep(x, x_num)))
  # construct sf dataframe
  gcm_grid <- st_sf(data.frame(x = x_cells, y = y_cells, cell_no = cell_nos), geometry=gcm_grid_sfc)

  return(gcm_grid)
}

# Construct grid tiles from a GCM netcdf
# NOTE - sf_make_grid() can only  make square grid polygons
# We're using tile_dim of 10 for now (tiles = 10 grid cells x 10 grid cells)
# The grid is 110 cells wide by 85 high, and in this function we
# get the number of rows/columns by floor(n_cells[high/wide]/tile_dim) so are rounding down.
# With a tile_dim of 10, tiles won't cover full height of grid
# but top 5 rows are fully outside CONUS, so we are dropping for now
# rather than constructing two separate sf grids and merging
construct_grid_tiles <- function(gcm_nc, tile_dim) {
  gcm_data <- ncdf4::nc_open(gcm_nc)

  # pull projection information
  gcm_crs <- pull_gcm_crs(gcm_data)

  # set grid size (there is a 'grid_size_in_meters' attribute,
  # but I can't seem to pull it with ncatt_get)
  grid_size_m <- 25000

  # pull projected coordinates from netcdf (note - in units of **kilometers**)
  y_coord <- ncdf4::ncvar_get(gcm_data, 'iy')
  x_coord <- ncdf4::ncvar_get(gcm_data, 'jx')

  # get number of grid cells in x and y dimensions
  y_num <- length(y_coord) - 1
  x_num <- length(x_coord) - 1

  # get offset (lower left corner of grid)
  # multiply by 1000 to get in units of meters, since crs unit is meters
  y_min <- y_coord[1]*1000
  x_min <- x_coord[1]*1000

  # determine the number of columns and rows of tiles
  # the grid is 110 wide by 85 tall
  # st_make_grid can only make square grids
  # the top 5 rows of the grid are fully outside of CONUS
  # so will exclude here by flooring y_rows
  # works b/c st_make_grid starts in lower left corner of grid
  xcolumns <- floor(x_num/tile_dim)
  yrows <- floor(y_num/tile_dim)

  # Build tiles
  gcm_tiles_sfc <- sf::st_make_grid(cellsize = grid_size_m*tile_dim, n = c(xcolumns, yrows), offset = c(x_min, y_min), crs = gcm_crs)
  # set up attributes for tile number, x and y grid values
  # tiles count left to right, then next row, then left to right
  tile_nos <- seq(1:(xcolumns*yrows))
  x_tiles <- rep(1:(xcolumns), yrows)
  y_tiles <- c(sapply(1:(yrows), function(x) rep(x, xcolumns)))
  # construct sf dataframe
  gcm_tiles <- st_sf(data.frame(x = x_tiles, y = y_tiles, tile_no = tile_nos), geometry=gcm_tiles_sfc)

  return(gcm_tiles)
}

# TODO: a much better solution. For now, this just subsets the lakes
# to 5 in WI, but it will need to do smart chunking at some point.
split_lake_centroids <- function(centroids_sf_rds) {

  set.seed(19) # Same subset of 5 every time
  wi_sf <- st_as_sf(maps::map('state', 'wisconsin', plot=FALSE, fill=TRUE))

  readRDS(centroids_sf_rds) %>%
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
