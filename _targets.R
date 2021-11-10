library(targets)

options(tidyverse.quiet = TRUE)
tar_option_set(packages = c("tidyverse", "sf", "scipiper"))


source('7_drivers_munge/src/GCM_driver_utils.R')

targets_list <- list(
  tar_target(
    centroids_sf,
    gd_get(ind_file = '2_crosswalk_munge/out/centroid_lakes_sf.rds.ind') %>% readRDS
  ),
  tar_target(
    centroid_map_png,
    map_centroids(centroids_sf, out_file = '7_drivers_munge/out/centroid_map.png'),
    format='file'
  )
)

# Return the complete list of targets
c(targets_list)
