
map_query <- function(out_file, centroids_sf) {

  # TODO: delete this WI-specific view
  wi_sf <- st_as_sf(maps::map('state', 'wisconsin', plot=FALSE, fill=TRUE))

  query_plot <- ggplot() +
    geom_sf(data=wi_sf) +
    geom_sf(data=centroids_sf, color='salmon', size=4) +
    coord_sf() + theme_void()

  ggsave(out_file, query_plot, width=10, height=8, dpi=300)

  return(out_file)
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

# Reproject to the crs of the gcm grid
project_to_grid_crs <- function(input_sf, grid_cells) {
  sf::st_transform(input_sf, crs = st_crs(grid_cells))
}


# need to subset grid tiles polygons to that with mapped tile id
# then subset grid cells to polygons within that tile
# using centroids for clean intersection
# then filtering cell polygons to those w/ subsetted ids
get_tile_cells <- function(grid_cell_centroids, grid_cells_sf, grid_tiles, grid_tile_id) {
  grid_tile <- grid_tiles %>%
    filter(tile_no == grid_tile_id)

  tile_cells <- grid_cell_centroids %>%
    st_intersection(grid_tile)

  tile_cell_ids <- tile_cells$cell_no

  grid_cells_sf %>%
    filter(cell_no %in% tile_cell_ids) %>%
    mutate(tile_no = grid_tile_id)
}

# Filter cells associated with given tile
# to only those cells that contain lakes
keep_cells_with_lakes <- function(tile_cells, lake_centroids) {
  cell_lakes_intersect <- st_intersects(tile_cells, lake_centroids)

  cells_w_lakes <- tile_cells %>% mutate(contains_lake = lengths(cell_lakes_intersect) > 0, n_lakes = lengths(cell_lakes_intersect)) %>%
    dplyr::filter(contains_lake)

  return(cells_w_lakes)

}


get_query_centroids <- function(query_cells, cell_centroids) {
  # remove cell_no column from cell_centroids, since don't need it
  # and would be duplicated on join
  cell_centroids <- cell_centroids %>% select(-cell_no)

  cell_centroids_w_lakes <- cell_centroids %>%
    st_join(query_cells, left=FALSE)

  return(cell_centroids_w_lakes)
}

# Get lake cell xwalk
get_lake_cell_xwalk <- function(lake_centroids, tile_cells) {
  lake_cells_join <- lake_centroids %>%
    st_join(tile_cells, left=FALSE) %>%
    st_set_geometry(NULL)

  return(lake_cells_join)
}


# Convert an sf object into a geoknife::simplegeom, so that
# it can be used in the geoknife query. `geoknife` only works
# with `sp` objects but not SpatialPoints at the moment, so
# need to convert these to a data.frame.
#
# Added an if statement to catch dataframes w/ 0 rows
# since using this to map over tiles that may not
# have any cells that contain lakes, and therefore
# no cell centroids to convertto simplegeom
sf_pts_to_simplegeom <- function(sf_obj) {
  if (nrow(sf_obj) > 0 ){
    sf_obj %>%
      st_coordinates() %>%
      t() %>% as.data.frame() %>%
      simplegeom()
  }
}

# Set up a download file to get the raw GCM data down.
# This function accepts each of the query parameters for the
# geoknife job as an argument. Currently missing the "knife" parameter.
download_gcm_data <- function(out_file, query_geom, query_url, query_vars,
                              query_dates, query_knife = NULL) {
  gcm_job <- geoknife(
    stencil = query_geom,
    fabric = webdata(
      url = query_url,
      variables = query_vars,
      times = query_dates
    )
  )

  wait(gcm_job)
  my_data <- result(gcm_job)
  arrow::write_feather(my_data, out_file)

  return(out_file)
}
