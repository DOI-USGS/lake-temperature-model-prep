
#' get point elevation data from the `elevatr` package
#'
#' @param point_rds spatial data file for points
#' @param zoom zoom level, input into `elevatr::get_aws_points` function
#' @param dummy character that isn't used but if changed, will cause rebuilds
get_ned_points <- function(point_rds, zoom, dummy){

  point_rds %>% readRDS() %>%
    elevatr::get_aws_points(z = zoom, verbose = FALSE) %>%
    suppressMessages() %>% {.[[1]]} %>%
    sf::st_drop_geometry() %>%
    dplyr::select(site_id, elevation)

}


bind_to_csv <- function(fileout, ...){
  bind_rows(...) %>% write_csv(fileout)
}
