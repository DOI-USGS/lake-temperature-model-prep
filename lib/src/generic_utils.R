
#' write a unique value to a text file when this function is called.
#' Replaces existing text in the file
#'
#' @param filepath a text filepath that can be written to
#'
#' @details this function is used to keep a dependency stale always, assuming this function is called whenever
#' that file is referenced and there is at least a paired target use of the file.
#' The file is kept stale by writing the current time (with timezone offset) and a random number. The random
#' number is used because this function could be called rapidly several times within the same second.
make_file_stale <- function(filepath){
  cat(file = filepath, paste0(format(Sys.time(), '%m/%d/%y %H:%M:%S %z; random ID: '), sample(1E6, 1)))
}
