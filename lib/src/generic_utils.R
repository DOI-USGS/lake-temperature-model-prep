
#' write a unique value to a text file when this function is called.
#' Replaces existing text in the file
#'
#' @param filepath a text filepath that can be written to
#'
#' @details this function is used to keep a dependency stale always, assuming this function is called whenever
#' that file is referenced and there is at least a paired target use of the file.
make_file_stale <- function(filepath){
  cat(file = filepath, paste0(format(Sys.time(), '%m/%d/%y %H:%M:%S; random ID: '), sample(1E6, 1)))
}
