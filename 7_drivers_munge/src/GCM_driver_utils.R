
map_centroids <- function(centroids_sf_rds, out_file) {
  centroids_sf <- readRDS(centroids_sf_rds)
  centroid_plot <- ggplot() +
    geom_sf(data=centroids_sf, color='dodgerblue', size=0.5)

  ggsave(out_file, centroid_plot, width=10, height=8, dpi=300)

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

# Take the lake centroids and convert into a polygon to query
# the Geo Data Portal for driver data.
centroids_to_poly <- function(centroids_sf) {
  centroids_sf %>%
    st_make_grid()
}

# Convert an sf object into a geoknife::simplegeom, so that
# it can be used in the geoknife query. Must become an
# `sp` object first.
sf_to_simplegeom <- function(sf_obj) {
  sf_obj %>%
    as_Spatial() %>%
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
  download(gcm_job, destination = out_file, overwrite = TRUE)

  return(out_file)
}
