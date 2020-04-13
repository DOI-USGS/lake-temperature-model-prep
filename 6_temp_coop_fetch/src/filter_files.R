filter_coop_all <- function(in_ind) {

  # this is just a filter to exclude files in gd that
  # should eventually be moved/cleaned up
  # for now, just keep all .xlsx, .txt, .csv, .accdb

  gd_files <- readRDS(sc_retrieve(in_ind))
  filenames <- gd_files$name

  return(filenames)
}
