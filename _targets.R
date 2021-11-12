library(targets)

options(tidyverse.quiet = TRUE)
tar_option_set(packages = c("tidyverse", "sf", "scipiper", "ggplot2"))


source('7_drivers_munge/src/GCM_driver_utils.R')

targets_list <- list(
  tar_target(
    centroids_sf_rds,
    gd_get('2_crosswalk_munge/out/centroid_lakes_sf.rds.ind'),
    format='file'
  ),
  tar_target(
    centroid_map_png,
    map_centroids(centroids_sf_rds, out_file = '7_drivers_munge/out/centroid_map.png'),
    format='file'
  ),
  tar_target(
    gcm_files_out,
    c(centroids_sf_rds, centroid_map_png)
  )
)

# Return the complete list of targets
c(targets_list)
