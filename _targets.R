library(targets)

options(tidyverse.quiet = TRUE)
tar_option_set(packages = c(
  "tidyverse",
  "sf",
  "scipiper",
  "ggplot2",
  "geoknife",
  "arrow"
))

source('7_drivers_munge/src/GCM_driver_utils.R')

targets_list <- list(
  # The input from our `scipiper` pipeline
  tar_target(
    centroids_sf_rds,
    gd_get('2_crosswalk_munge/out/centroid_lakes_sf.rds.ind'),
    format='file'
  ),

  tar_target(
    query_centroids_sf,
    split_lake_centroids(centroids_sf_rds)
  ),

  # Convert the sf centroids into a geoknife-ready format
  tar_target(
    query_centroids_geoknife,
    sf_pts_to_simplegeom(query_centroids_sf)
  ),

  # TODO: make this parameterized/branched. Right now, just doing
  # a single GCM, but will want to parameterize to each of them +
  # handle chunks of lakes. See this config file example for an example:
  # https://github.com/USGS-R/necsc-lake-modeling/blob/a81a0e7ed6ede66253f765b42568a4d39a5dccc2/configs/ACCESS_config.yml
  tar_target(
    gcm_data_raw_feather,
    download_gcm_data(
      out_file = "7_drivers_munge/tmp/7_GCM_GFDL_raw.feather",
      query_geom = query_centroids_geoknife,
      query_url = "https://cida.usgs.gov/thredds/dodsC/notaro_GFDL_1980_1999",
      # Data definitions: https://cida.usgs.gov/thredds/ncss/notaro_GFDL_2040_2059/dataset.html
      query_vars = c("evspsbl"), # TODO: more than one var, but geoknife isn't working with that
      query_dates = c('1999-01-01', '1999-01-15')
    ),
    format = "file"
  ),

  tar_target(
    query_map_png,
    map_query(
      out_file = '7_drivers_munge/out/query_map.png',
      centroids_sf = query_centroids_sf
    ),
    format='file'
  ),
  tar_target(
    gcm_files_out,
    c(gcm_data_raw_txt, query_map_png)
  )
)

# Return the complete list of targets
c(targets_list)
