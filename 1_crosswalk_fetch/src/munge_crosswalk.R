#' @param out_ind indicator file to write for output
#' @param shp_ind .ind of the shapefile .shp file to use in reading
#' @param ... other indicator files for those files (not explicitly inspected)
#'   making up the shapefile
munge_crosswalk <- function(out_ind, shp_ind, ...) {
  # read the file
  shp <- sf::st_read(scipiper::sc_retrieve(shp_ind))

  # munging could happen here

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(shp, data_file)
  gd_put(out_ind, data_file)
}
