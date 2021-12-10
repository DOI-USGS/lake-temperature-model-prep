# function to retrieve list of files from google drive

#' @param trigger_file specify a filepath to keep this target stale.
#' Omit (NULL) to avoid rebuilds
#' @param trigger_wait number of minutes to wait between making this stale again
#'
files_in_drive <- function(out_ind, gd_path, trigger_file = NULL, trigger_wait) {

  if (!is.null(trigger_file)){
    make_file_stale(trigger_file)
  }

  # check time since last write. Is it too recent? then skip
  # if ind doesn't exist, hit google; if freshened doesn't exist, hit google;
  # if isn't fresh enough, hit google
  if (file.exists(out_ind)){
    ind_data <- yaml::yaml.load_file(out_ind)
    if (is.null(ind_data$freshened) ||
        (difftime(Sys.time(), as.POSIXct(ind_data$freshened), units = 'mins')) > trigger_wait){
      gd_freshen_dir(out_ind, gd_path)
    } else {
      message('skipping google drive index because it still smells fresh')
    }
  } else {
    # ind doesn't exist, perhaps due to `scdel()` or a manual delete
    gd_freshen_dir(out_ind, gd_path)
  }
}

# this is the expensive step. And we also don't want to get limited by
# google's API.
gd_freshen_dir <- function(out_ind, gd_path){

  gd_files <- drive_ls(path = gd_path)

  saveRDS(gd_files, file = as_data_file(out_ind))
  gd_put(out_ind)
  ind_data <- yaml::yaml.load_file(out_ind)
  ind_data$freshened <- format(Sys.time(), '%Y-%m-%d %H:%M:%S %z')
  # write in same way scipiper does sc_indicate:
  readr::write_lines(yaml::as.yaml(ind_data), out_ind)
}
