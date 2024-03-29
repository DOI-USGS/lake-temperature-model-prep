
#' create a polygon cell grid from specifications. Assumes lat/lon data
#' and rectangular polygons.
#'
#' @param x0 the left edge of the cell grid to be built
#' @param y0 the lower edge of the cell grid to be built
#' @param x_num the number of cells in the x dimension
#' @param y_num the number of cells in the y dimension
#' @param cell_res the resolution (width and height) of the cells
#'
#' @return an sf data.frame with x and y attributes, specifying
#' cell indices (0 indexed)
create_ldas_grid <- function(x0, y0, x_num, y_num, cell_res){
  ldas_crs <- "+init=epsg:4326"

  ldas_grid_sfc <- sf::st_make_grid(cellsize = cell_res, n = c(x_num, y_num),
                                    offset = c(x0-cell_res/2, y0-cell_res/2), crs = ldas_crs)
  # cells count left to right, then next row, then left to right
  x_cells <- rep(0:(x_num-1), y_num)
  y_cells <- c(sapply(0:(y_num-1), function(x) rep(x, x_num)))

  ldas_grid <- st_sf(data.frame(x = x_cells, y = y_cells), ldas_grid_sfc)
  return(ldas_grid)
}



#' get the x and y indices of cells that contain points
#'
#' @param cell_grid an `st_sf` of polygons with fields "x" and "y"
#' @param points spatial points sharing the same CRS as `cell_grid`
#'
#' @return a data.frame with x and y fields that correspond to cell indices
cells_containing_points <- function(cell_grid, points){
  grid_contains <- st_intersects(cell_grid, points) # intersects (instead of st_contains) covers the edge case where the point lies right on an edge or vertex

  cells_w_pts <- cell_grid %>% mutate(contains_point = lengths(grid_contains) > 0) %>%
    dplyr::filter(contains_point) %>% st_set_geometry(NULL) %>%
    dplyr::select(x, y)
  return(cells_w_pts)
}

cells_containing_points_within <- function(cell_grid, points, x_range, y_range){
  cells_containing_points(cell_grid, points) %>%
    dplyr::filter(x_range[1] <= x & x <= x_range[2], y_range[1] <= y & y <= y_range[2])
}


sf_file_centroids <- function(filepath){

  .obj <- readRDS(filepath)
  st_centroid(.obj)
}

as_OPeNDAP_cells <- function(cell_indices_df, variables){
  x_cells <- cell_indices_df$x
  y_cells <- cell_indices_df$y
  list(x = x_cells, y = y_cells, variables = variables) %>%
    cell_list_to_df()
}

