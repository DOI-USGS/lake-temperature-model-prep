
#' get point elevation data from the `elevatr` package
#'
#' @param point_rds spatial data file for points
#' @param zoom zoom level, input into `elevatr::get_aws_points` function
#' @param dummy character that isn't used but if changed, will cause rebuilds
#'
#' @details
#' needed to install CRAN binary of sf and downgrade PROJ/GDAL to get past error:
#' Error in sf::gdal_utils(util = "warp", source = files,
#' destination = destfile,  : gdal_utils warp: an error occured
#' Captured later here: https://github.com/jhollist/elevatr/issues/62
get_ned_points <- function(point_rds, zoom, dummy){

  point_rds %>% readRDS() %>%
    elevatr::get_aws_points(z = zoom, verbose = FALSE) %>%
    suppressMessages() %>% {.[[1]]} %>%
    sf::st_drop_geometry() %>%
    dplyr::select(site_id, elevation)

}


bind_to_feather <- function(fileout, ...){
  bind_rows(...) %>% arrow::write_feather(fileout)
}
