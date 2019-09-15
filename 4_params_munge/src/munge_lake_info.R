

munge_lake_area <- function(out_ind, lakes_ind){

  lakes <- scipiper::sc_retrieve(lakes_ind) %>% readRDS()
  areas <- lakes %>%
    st_area()

  lake_areas <- data.frame(site_id = lakes$site_id, areas_m2 = as.numeric(areas)) # Units: [m^2]

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lake_areas, data_file)
  gd_put(out_ind, data_file)
}
