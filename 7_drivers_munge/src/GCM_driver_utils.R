
map_query <- function(out_file, centroids_sf, polys_sf) {

  # TODO: delete this WI-specific view
  wi_sf <- st_as_sf(maps::map('state', 'wisconsin', plot=FALSE, fill=TRUE))

  query_plot <- ggplot() +
    geom_sf(data=wi_sf) +
    geom_sf(data=polys_sf, color='dodgerblue', fill=NA, size=1) +
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
