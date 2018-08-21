#' Throw an error if ind_file does not have a matching local data_file
#'
#' Experimenting with this function, which could make it into scipiper. If this
#' works, it should allow us to have a chain of .ind files, each depending on
#' the next, without requiring that the corresponding data_files all exist on
#' the shared cache OR that all collaborators build/have the file locally. Only
#' collaborators who need to use the contents should need to build it.
#'
#' @param ind_file indicator file for which to require a local version.
require_local <- function(ind_file) {
  if(!all(scipiper::is_ind_file(ind_file))) {
    non_ind_file <- ind_file[!scipiper::is_ind_file(ind_file)]
    stop("Not an ind_file: ", paste0(non_ind_file, collapse=', '))
  }

  data_file <- scipiper::as_data_file(ind_file)
  data_files_exist <- file.exists(data_file)
  if(!all(data_files_exist)) {
    stop(sprintf(
      "Missing a local copy of data_file[s]: %s. Force rebuild of the ind_files to use these locally.",
      paste0(data_file[!data_files_exist], collapse=', ')))
  }

  invisible()
}
