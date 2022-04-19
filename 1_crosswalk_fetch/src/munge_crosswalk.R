#' @param out_ind indicator file to write for output
#' @param shp_ind .ind of the shapefile .shp file to use in reading
#' @param ... other indicator files for those files (not explicitly inspected)
#'   making up the shapefile
munge_crosswalk <- function(out_ind, shp_ind, ...) {
  # read the file
  shp <- sf::st_read(scipiper::sc_retrieve(shp_ind)) %>% dplyr::select(site_id)

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
#' @param states state IDs to include (e.g., "SD" for South Dakota)
#'
MGLP_zip_to_sf <- function(out_ind, gdb_file, zip_ind, states){
  zip_file <- scipiper::sc_retrieve(zip_ind)

  shp.path <- tempdir()
  unzip(zip_file, exdir = shp.path)

  shp <- sf::st_read(file.path(shp.path, gdb_file), layer = 'MGLP_LAKES') %>%
    mutate(site_id = paste0('MGLP_', LAKE_ID)) %>% dplyr::select(site_id, geometry = SHAPE) %>% # why do I need to rename SHAPE to geometry??
    st_transform(x, crs = 4326)


  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(shp, data_file)
  gd_put(out_ind, data_file)
}

#' Use the LAGOS-US to NHDHR crosswalk provided in the LAGOS-US LOCUS
#' module at https://doi.org/10.6073/pasta/e5c2fb8d77467d3f03de4667ac2173ca
#' Downloaded the `lake_information.csv` file and saved in our pipeline
#' as `1_crosswalk_fetch/in/LAGOS_US_lake_information.csv`
lagos_to_xwalk <- function(out_ind, lakeinfo_ind, states){

  lakeinfo_file <- scipiper::sc_retrieve(lakeinfo_ind)

  lagos_nhdhr_xwalk <- readr::read_csv(lakeinfo_file) %>%
    dplyr::filter(lake_states %in% states) %>%
    dplyr::mutate(LAGOS_ID = sprintf("lagos_%s", lagoslakeid),
                  site_id = sprintf("nhdhr_%s", lake_nhdid)) %>%
    dplyr::select(LAGOS_ID, site_id)

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lagos_nhdhr_xwalk, data_file)
  gd_put(out_ind, data_file)
}

#' @param out_ind indicator file to write for output
#' @param lake_ind canonical lake feature collection indicator file
munge_names <- function(out_ind, lake_ind){

  stop("this will break because we aren't carrying the lake names w/ the canonical sf")
  # get lake info
  lakes_sf <- readRDS(scipiper::sc_retrieve(lake_ind))

  lakes_sf_info <- data.frame(lake_name = lakes_sf$GNIS_Nm,
                              site_id = lakes_sf$site_id, stringsAsFactors = FALSE)

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lakes_sf_info, data_file)
  gd_put(out_ind, data_file)
}
