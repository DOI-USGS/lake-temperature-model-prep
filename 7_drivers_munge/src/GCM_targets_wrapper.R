build_GCM_pipeline <- function(ind_file, ...) {

  tar_make()

  tar_load(pipeline_files_out)
  sc_indicate(ind_file, data_file=pipeline_files_out)
}
