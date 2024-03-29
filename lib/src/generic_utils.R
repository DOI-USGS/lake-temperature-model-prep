#' Convert from fahrenheit to celsius
#'
#' Convert temperatures from fahrenheit to celsius using a consistent conversion value
#'
#' @x num, temperature (deg F)
#'
fahrenheit_to_celsius <- function(x){ 5/9*(x - 32) }

#' Convert from feet to meters
#'
#' Convert values from feet to meters using a consistent conversion value
#'
#' @x num, depth (ft)
#'
convert_ft_to_m <- function(x) {
  x * 0.3048
}

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
  cat(file = filepath, paste0(format(Sys.time(), '%Y-%m-%d %H:%M:%S %z; random ID: '), sample(1E6, 1), '\n'))
}
