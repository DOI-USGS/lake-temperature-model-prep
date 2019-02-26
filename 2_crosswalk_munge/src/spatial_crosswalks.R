
crosswalk_points_in_poly <- function(ind_file, poly_ind_file, points_ind_file){
  poly_data <- gd_get(ind_file = poly_ind_file) %>% readRDS
  points_data <- gd_get(ind_file = points_ind_file) %>% readRDS

  stopifnot('site_id' %in% names(poly_data))
  stopifnot('MonitoringLocationIdentifier' %in% names(points_data))

  crosswalked_points <- st_join(points_data, poly_data, join = st_intersects) %>%
    filter(!is.na(site_id))

  crosswalked_ids <- data.frame(
    MonitoringLocationIdentifier = crosswalked_points$MonitoringLocationIdentifier,
    site_id = crosswalked_points$site_id)

  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(crosswalked_ids, data_file)
  gd_put(ind_file, data_file)
}


combine_sf_lakes <- function(out_ind, ...){
  sf_inds <- c(...)

  sf_lakes <- readRDS(scipiper::sc_retrieve(sf_inds[1L]))
  for (sf_ind in tail(sf_inds, -1)){
    sf_lakes <- rbind(sf_lakes, readRDS(scipiper::sc_retrieve(sf_ind)))
  }
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(sf_lakes, data_file)
  gd_put(out_ind, data_file)
}
