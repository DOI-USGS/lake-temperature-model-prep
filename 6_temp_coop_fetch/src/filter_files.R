#' @param trigger_file specify a filepath to keep this target stale.
#' Omit (NULL) to avoid rebuilds
filter_coop_all <- function(in_tind, trigger_file) {

  # this is just a filter to exclude files in gd that
  # should eventually be moved/cleaned up
  # for now, just keep all .xlsx, .txt, .csv, .accdb

  if (!is.null(trigger_file)){
    make_file_stale(trigger_file)
  }


  # in_tind is a LOCAL, time-based indicator file. It does not represent a cache file
  gd_files <- readRDS(as_data_file(in_tind, ind_ext = 'tind'))
  filenames <- gd_files$name

  return(filenames)
}
