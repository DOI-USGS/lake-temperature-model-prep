#' @param out_ind indicator file to write for output
#' @param shp_ind .ind of the shapefile .shp file to use in reading
#' @param ... other indicator files for those files (not explicitly inspected)
#'   making up the shapefile
munge_crosswalk <- function(out_ind, shp_ind, ...) {
  # read the file
  shp <- sf::st_read(scipiper::sc_retrieve(shp_ind)) %>% select(site_id)

  # munging could happen here

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(shp, data_file)
  gd_put(out_ind, data_file)
}


#' we take the MGLP geodatabase and read it into sf,
#' then remove lakes that aren't part of the assessment and/or aren't
#' part of the expansion footprint, as defined by state IDs in ...
#'
#' @param ... state IDs to include (e.g., "SD" for South Dakota)
#'
MGLP_zip_to_sf <- function(out_ind, gdb_file, zip_ind, ...){
  states <- c(...)
  zip_file <- scipiper::sc_retrieve(zip_ind)

  shp.path <- tempdir()
  unzip(zip_file, exdir = shp.path)

  shp <- sf::st_read(file.path(shp.path, gdb_file), layer = 'MGLP_LAKES') %>%
    filter(ASSESS == 'Y', STATE %in% states) %>%
    mutate(site_id = paste0('mglp_', LAKE_ID)) %>% dplyr::select(site_id, geometry = SHAPE) %>% # why do I need to rename SHAPE to geometry??
    st_transform(x, crs = 4326)


  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(shp, data_file)
  gd_put(out_ind, data_file)
}


LAGOS_zip_to_sf <- function(out_ind, layer, zip_ind, ...){


  states <- c(...)
  zip_file <- scipiper::sc_retrieve(zip_ind)

  shp.path <- tempdir()
  unzip(zip_file, exdir = shp.path)

  shp <- sf::st_read(shp.path, layer = layer) %>%
    filter(STATE %in% states) %>%
    mutate(site_id = paste0('lagos_', lagoslakei)) %>% dplyr::select(site_id, geometry) %>%
    st_transform(x, crs = 4326)


  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(shp, data_file)
  gd_put(out_ind, data_file)
}
