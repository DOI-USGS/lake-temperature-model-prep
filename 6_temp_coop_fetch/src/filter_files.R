#' @param trigger_file specify a filepath to keep this target stale.
#' Omit (NULL) to avoid rebuilds
filter_coop_all <- function(in_ind, trigger_file) {

  # this is just a filter to exclude files in gd that
  # should eventually be moved/cleaned up
  # for now, just keep all .xlsx, .txt, .csv, .accdb

  if (!is.null(trigger_file)){
    make_file_stale(trigger_file)
  }


  gd_files <- readRDS(sc_retrieve(in_ind))
  filenames <- gd_files$name

  return(filenames)
}
