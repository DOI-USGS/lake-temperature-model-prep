# function to retrieve list of files from google drive

#' @param trigger_file specify a filepath to keep this target stale.
#' Omit (NULL) to avoid rebuilds
#' @param trigger_wait number of minutes to wait between making this stale again
#'
#'
#' IAAAAA should we be doing this? probably not
files_in_drive <- function(out_ind, gd_path, trigger_file = NULL, trigger_wait) {

  if (!is.null(trigger_file)){
    time_since_stale <- readLines(trigger_file, n =  1) %>%
      strsplit(';') %>% {.[[1]][1]} %>%
      {Sys.time() - as.POSIXct(.)} %>% as.numeric()
    if (time_since_stale > trigger_wait)
      make_file_stale(trigger_file)
  }

  gd_files <- drive_ls(path = gd_path)

  saveRDS(gd_files, file = as_data_file(out_ind))
  gd_put(out_ind)

}
