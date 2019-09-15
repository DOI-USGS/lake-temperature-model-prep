

munge_lake_area <- function(out_ind, lakes_ind){

  lakes <- scipiper::sc_retrieve(lakes_ind) %>% readRDS()
  areas <- lakes %>% # should probably do this as a mutate to keep it w/ site_id??
    st_area()

  lake_areas <- data.frame(site_id = lakes$site_id, areas_m2 = as.numeric(areas)) # Units: [m^2]

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lake_areas, data_file)
  gd_put(out_ind, data_file)
}


munge_lat_lon <- function(out_ind, centroids_ind){
  centroids_sf <- scipiper::sc_retrieve(centroids_ind) %>% readRDS()

  # dangerous as it assumes order is retained as we de-couple `site_id`
  lat_lon <- st_coordinates(centroids_sf) %>% as_tibble() %>%
    rename(longitude = X, latitude = Y) %>% mutate(site_id = centroids_sf$site_id) %>%
    dplyr::select(site_id, everything())

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lat_lon, data_file)
  gd_put(out_ind, data_file)
}
