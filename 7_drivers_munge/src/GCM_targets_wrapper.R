build_GCM_pipeline <- function(ind_file, ...) {

  tar_make()

  tar_load(centroid_map_png)
  sc_indicate(ind_file, data_file=c(centroid_map_png))
}
