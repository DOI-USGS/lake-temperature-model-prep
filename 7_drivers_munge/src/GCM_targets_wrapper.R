build_GCM_pipeline <- function(ind_file, ...) {
  # Build the targets pipeline
  tar_make(gcm_files_out)

  # Load the final target that lists the output files
  tar_load(gcm_files_out)

  # Indicate those file names and hashes
  sc_indicate(ind_file, data_file=gcm_files_out)
}
