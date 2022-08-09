
#' @title Create an `sf` objecting with square grid cells.
#' @description Reconstruct GCM grid using hard-coded grid parameters.
#' @param grid_params list with five elements: `crs`, `cellsize`,
#' `xmin`, `ymin`, `nx`, and `ny`. See descriptions of these elements
#' in the documentation for `construct_grid()` below.
reconstruct_gcm_grid <- function(grid_params) {
  # Build GCM grid
  gcm_grid <- construct_grid(cellsize = grid_params$cellsize,
                             nx = grid_params$nx,
                             ny = grid_params$ny,
                             xoffset = grid_params$xmin - grid_params$cellsize/2,
                             yoffset = grid_params$ymin - grid_params$cellsize/2,
                             crs = grid_params$crs) %>%
    rename(cell_no = id)

  return(gcm_grid)
}

#' @title Create an `sf` objecting representing groups of square
#' grid cells that we are calling "tiles".
#' @description Construct grid tiles (groups of grid cells) using
#' hard-coded grid parameters. Grid tiles can be used to group
#' queries to the Geodata Portal into more manageable sizes.
#' @param grid_params list with five elements: `crs`, `cellsize`,
#' `xmin`, `ymin`, `nx`, and `ny`. See descriptions of these elements
#' in the documentation for `construct_grid()` below.
#' @param tile_dim single numeric value representing how many grid cells to
#' group into a single tile. NOTE - sf_make_grid() can only make square grid
#' polygons. The GCM grid is 217 cells wide by 141 high. With a `tile_dim` of
#' 15 (tiles = 15 grid cells x 15 grid cells), tiles won't cover full height
#' of the GCM grid and there will be 7 columns left out on the right and 5 left
#' out on the top. Since all of the cells that are missing are fully outside CONUS,
#' we are OK with dropping for now. If we wanted to include, we would need to
#' construct two separate `sf` grids and merge.
construct_grid_tiles <- function(grid_params, tile_dim) {
  # determine the number of columns and rows of tiles
  xcolumns <- floor(grid_params$nx/tile_dim)
  yrows <- floor(grid_params$ny/tile_dim)

  gcm_tiles <- construct_grid(cellsize = grid_params$cellsize*tile_dim,
                              nx = xcolumns,
                              ny = yrows,
                              # Tile bottomleft should start at bottomleft cell
                              # corner, but xmin/ymin is the cell centroid
                              xoffset = grid_params$xmin - grid_params$cellsize/2,
                              yoffset = grid_params$ymin - grid_params$cellsize/2,
                              crs = grid_params$crs) %>%
    rename(tile_no = id)

  return(gcm_tiles)
}

#' @title Create an `sf` object representing a square grid
#' @description Shared function used to generate a grid based on grid cell size, dimensions,
#' placement, and projection to use. This is used by both `reconstruct_gcm_grid()`
#' and `construct_grid_tiles()` to generate grids with their appropriate configurations.
#' @param cellsize numeric value representing the dimensions of the square grid cell in meters.
#' @param nx number of cells to place in the x direction
#' @param ny number of cells to place in the y direction
#' @param xoffset x dimension for the bottomleft corner of the grid
#' @param yoffset y dimension for the bottomleft corner of the grid
#' @param crs character string representing the projection of the grid
construct_grid <- function(cellsize, nx, ny, xoffset, yoffset, crs) {

  # Build grid
  grid_sfc <- sf::st_make_grid(cellsize = cellsize,
                               n = c(nx, ny),
                               # Use the bottomleft corner of the bottomleft
                               # cell and not its centroid.
                               offset = c(xoffset, yoffset),
                               crs = crs)

  # set up attributes for cell number, x and y grid values
  # cells count left to right, then next row, then left to right
  cell_nums <- seq(1:(nx*ny))
  xcells <- rep(1:nx, ny)
  ycells <- c(sapply(1:ny, function(x) rep(x, nx)))

  # construct sf dataframe
  grid_sf <- st_sf(data.frame(x = xcells, y = ycells, id = cell_nums), geometry=grid_sfc)

  return(grid_sf)
}

#' @title Match grid cells to the grid tiles.
#' @description Using grid cell centroids, this spatially
#' intersects the cells to the bigger tiles and returns a
#' non-spatial data.frame with `cell_no` and `tile_no`
#' columns to map each grid cell to the tile they are inside.
#' @param grid_cell_centroids an `sf` object of points representing
#' the grid cell centroids. Must contain a `cell_no` column which has
#' the id of each of the cell centroids
#' @param grid_tiles an `sf` object with polygons representing the
#' groups of grid cells. Must contain a `tile_no` column which has
#' the id of each of the tile polygons.
get_cell_tile_xwalk <- function(grid_cell_centroids, grid_tiles) {
  # Figure out which cell centroids fall within each tile
  # Don't need to keep geometry
  grid_cell_centroids %>%
    st_intersection(grid_tiles) %>%
    st_drop_geometry() %>%
    select(cell_no, tile_no)
}

#' @title Filter grid cells to only those cells that contain lakes
#' @description Using an sf object of the grid cells and an sf object
#' of the lake centroids, this returns a data.frame with the number
#' of lakes contained in each cell, `n_lakes` and the `cell_no` for
#' any cell with at least one lake inside.
#' @param grid_cells an `sf` object of the square grid polygons. Assumes
#' that this is already in the same projection as `lake_centroids`. Must
#' contain a column called `cell_no`.
#' @param lake_centroids an `sf` object of points representing the
#' centroid for each lake. Assumes that this is already in the same
#' projection as `grid_cells`.
get_query_cells <- function(grid_cells, lake_centroids) {
  # Intersect the lake centroids with the grid cells.
  cell_lakes_intersect <- st_intersects(grid_cells, lake_centroids)

  # Keep only cells where there was at least one lake
  cells_w_lakes <- grid_cells %>%
    # Add column to say how many lakes the cell contains
    dplyr::mutate(n_lakes = lengths(cell_lakes_intersect)) %>%
    # Reorder the new column so that it is right after cell_no
    dplyr::relocate(n_lakes, .after = cell_no) %>%
    # Then, filter only to those containing at least one lake
    dplyr::filter(n_lakes > 0) %>%
    # Don't include geometry
    st_drop_geometry()

  return(cells_w_lakes)
}

#' @title Spatially match lakes to the grid cells and tiles.
#' @description Using lake centroids, this spatially intersects the lakes to
#' the grid cells and returns a non-spatial data.frame with `site_id`,
#' `state`, `cell_no` and `tile_no`, as the function also joins the
#' cell_tile xwalk in order to add the tile number for each lake.
#' @param lake_centroids an `sf` object of points representing the lake
#' centroids.
#' @param grid_cells an `sf` object with square polygons representing the
#' grid cells. Must contain a `cell_no` column which has
#' the id of each of the cell polygons.
#' @param cell_tile_xwalk mapping of which cells are in which tiles
#' @return An output table with the fields `site_id`, `state`, `cell_no` and
#' `tile_no`
get_lake_cell_tile_spatial_xwalk <- function(lake_centroids, grid_cells, cell_tile_xwalk) {
  lake_cells_tiles_xwalk <- lake_centroids %>%
    st_join(grid_cells, left=FALSE) %>%
    st_drop_geometry() %>%
    left_join(cell_tile_xwalk, by='cell_no') %>%
    select(site_id, state, cell_no, tile_no)

  return(lake_cells_tiles_xwalk)
}

#' @title Adjust the lake-cell-tile xwalk by matching lakes to the queried cells
#' that returned data.
#' @description Using lake centroids, this spatially matches lakes that fall within the
#' bounding box of the returned GCM data to query cells that returned data. For lakes
#' within the bounding box, if the cell that the lake falls within is not missing data,
#' the lake will be matched to the cell that it falls within. If the cell that the
#' lake falls within is missing data, the lake will be matched to a cell that did
#' return data. The preference is to match such lakes to non-nan cells with the
#' same y value (within the same row) as the cell that the lake falls within (as
#' is captured in the `spatial_xwalk` returned by `get_lake_cell_tile_spatial_xwalk`).
#' However, we do not want to match lakes to cells that are too far in the x direction
#' (too many columns away) from the cell that the lake falls within. The variable
#' `x_buffer` sets the maximum allowable x-distance (number of columns) between the
#' cell the lake is in and any potential non-nan cells that could be matched to the
#' lake. If no non-nan cells are in the same row as the cell that is missing data,
#' or if there are no non-nan cells in that same row that are within the specified
#' x-distance, the lake will be matched to the non-nan cell with the centroid closest
#' to the lake centroid, regardless of y value. The output of this function will only
#' differ from the output of `get_lake_cell_tile_spatial_xwalk()` for those lakes that
#' fell within cells that did not return data.
#' @param spatial_xwalk mapping of which lakes are in which cells and tiles,
#' based on a spatial join
#' @param lake_centroids an `sf` object of points representing the lake
#' centroids.
#' @param grid_cells a `sf` object with square polygons representing the grid cells.
#' @param query_cell_centroids  an `sf` object of points representing the
#' grid cell centroids for queried cells. Must contain a `cell_no` column
#' which has the id of each of the cell centroids
#' @param cell_info a table with one row per query cell-gcm combo, with columns for
#' `gcm`, `tile_no`, `cell_no` and a column for `missing_data` indicating whether
#' or not the cell is missing data for any variable for that gcm. This table is
#' used here to filter the grid cell centroids to only those that returned data
#' @param x_buffer the maximum x distance (in columns) that any non-nan cells within
#' the same row as the originally matched cell can be in order to be in the pool of
#' cells to be matched to a given lake.
#' @return An output table with the fields `site_id`, `state`, `spatial_cell_no`,
#' `spatial_tile_no`, `data_cell_no` and `data_tile_no`. The 'spatial_' prefix
#' denotes the original spatial matching to all grid cells, while the 'data_' prefix
#' denotes the adjusted matching to non-nan grid cells.
adjust_lake_cell_tile_xwalk <- function(spatial_xwalk, lake_centroids, grid_cells, query_cell_centroids, cell_info, x_buffer) {
  # Pivot the cell_info tibble wider so that we have a single row per cell
  # and can track for how many gcms each cell is or is not missing data
  cell_status <- cell_info %>%
    pivot_wider(names_from='gcm', values_from='missing_data', names_glue="{gcm}_missing_data") %>%
    mutate(n_gcm_missing_data = rowSums(across(c(-cell_no,-tile_no))))

  # determine which cells returned data for all 6 GCMs
  cells_with_data <- cell_status %>%
    filter(n_gcm_missing_data == 0) %>%
    pull(cell_no)

  # filter the cell centroids to only those cells with data
  cell_centroids_with_data <- query_cell_centroids %>%
    filter(cell_no %in% cells_with_data)

  # get bounding box of grid cell polygons that returned data
  cells_with_data_bbox <- grid_cells %>%
    filter(cell_no %in% cells_with_data) %>%
    st_bbox() %>%
    st_as_sfc() %>%
    st_as_sf()

  # pull out Lake of the Woods. It falls just outside of the bounding box of returned data,
  # but has lots of data, so we want to keep it in our lake subset
  lotw <- filter(lake_centroids, site_id=='nhdhr_123319728')

  # subset lakes to only those within the bounding box of the returned data
  lake_centroids <- lake_centroids %>%
    st_join(cells_with_data_bbox, join=st_within, left=FALSE)

  # add back in Lake of the Woods
  lake_centroids <- bind_rows(lake_centroids, lotw)

  # match each lake to a cell that returned data. If the cell that the lake falls within
  # is not missing data, the lake will be matched to the cell that it falls within. If the
  # cell that the lake falls within is missing data, the lake will be matched to a cell
  # that returned data, preferably to a cell 1) within the same row as, and 2) within
  # `x_buffer` x distance (number of columns) from the cell that the falls within. If no
  # non-nan cells meet that criteria, the lake will be matched instead to the closest
  # non-nan cell, regardless of row
  adjusted_xwalk <- lake_centroids %>%
    left_join(spatial_xwalk, by=c('site_id', 'state')) %>% # add cell_no, tile_no for each lake
    left_join(query_cell_centroids %>% st_drop_geometry(), by=c('cell_no','tile_no')) %>% # add x, y coords of cell_no for each lake
    rename(spatial_x=x, spatial_y=y, spatial_cell_no=cell_no, spatial_tile_no=tile_no) %>%
    # Group by the y and x coordinates of the cell that the lake falls within...
    group_by(spatial_y, spatial_x) %>%
    # then use group_modify() to match the subset of lakes within that cell to non-nan cells.
    # Within the group_modify() call `.x` is the subset of the data for the group, while
    # `.y` is a tibble with one row and columns for each grouping variable (here, those
    # are`spatial_y` and `spatial_x`)
    group_modify(~ {
      # first, subset the non-nan cells to those with the same y value (in the same row) as the cell with missing data
      # and those within the specified x_buffer distance (+/- x_buffer columns) from the cell with missing data
      cells_in_row_and_within_x_buffer <- cell_centroids_with_data %>% filter(y==.y$spatial_y & abs(x-.y$spatial_x) <= x_buffer)
      # if there are non-nan cells that meet that criteria, match each of the lakes to the closest of that subset of cells
      # if no non-nan cells meet that criteria, instead match each lake to the nearest non-nan cell regardless of y value
      cells_to_match <- if (nrow(cells_in_row_and_within_x_buffer) > 0) cells_in_row_and_within_x_buffer else cell_centroids_with_data
      .x %>% st_join(cells_to_match,
                     join=st_nearest_feature, left=TRUE)
    }) %>%
    ungroup() %>%
    select(site_id, state, spatial_cell_no, spatial_tile_no, data_cell_no=cell_no, data_tile_no=tile_no)

  return(adjusted_xwalk)
}

#' @title Map the tiles and cells
#' @description Map the grid cells w/ lakes (symbolized by n_lakes per cell) and grid tiles
#' included in the GDP query
#' @param out_file name of output png file
#' @param lake_cell_tile_xwalk mapping of which lakes are in which cells and tiles
#' @param query_tiles vector of tiles that contain `query_cells`
#' @param query_cells vector of cells that contain lakes
#' @param grid_tiles an`sf` object with polygons representing the
#' groups of grid cells. Must contain a `tile_no` column which has
#' the id of each of the tile polygons.
#' @param grid_cells an `sf` object with square polygons representing the
#' grid cells. Must contain a `cell_no` column which has
#' the id of each of the cell polygons.
#' @return a png of the xwalk grid cells, symbolized by n_lakes per cell, and the grid tiles
# TODO: This function to should be 1) moved to 8_viz, and 2) made so that it only rebuilds if
# new cells will be plotted.
map_tiles_cells <- function(out_file, lake_cell_tile_xwalk, query_tiles, query_cells, grid_tiles, grid_cells) {
  lakes_per_cell <- lake_cell_tile_xwalk %>%
    group_by(cell_no) %>%
    summarize(nlakes = n())

  grid_tiles <- grid_tiles %>%
    filter(tile_no %in% query_tiles) %>% 
    st_transform(usmap::usmap_crs())

  tile_labels <- grid_tiles %>%
    mutate(bbox=split(.,tile_no) %>% purrr::map(sf::st_bbox)) %>%
    unnest_wider(bbox)

  grid_cells <- grid_cells %>%
    filter(cell_no %in% query_cells) %>%
    left_join(lakes_per_cell) %>% 
    st_transform(usmap::usmap_crs())
  
  tile_cell_plot <- usmap::plot_usmap(exclude = c("AK", "HI"), fill=NA) +
    geom_sf(data = grid_cells, aes(fill = nlakes)) +
    scico::scale_fill_scico(palette = "batlow", direction = -1) +
    geom_sf(data = grid_tiles, fill = NA, size = 2) +
    geom_text(data = tile_labels, aes(label=tile_no,x=xmin, y=ymax), size=8, nudge_x = 15000, nudge_y = -90000, hjust = 0) +
    theme(axis.title.y=element_blank(),
        axis.title.x=element_blank())

  # save file
  ggsave(out_file, tile_cell_plot, width=10, height=8, dpi=300, bg="white")

  return(out_file)
}

#' @title Map the missing cells
#' @description Map the grid cells w/ lakes that did not have any driver data returned
#' and indicate which cell they are using instead.
#' @param out_file name of output png file
#' @param lake_cell_tile_xwalk mapping of which lakes are in which cells and what their spatial
#' cell vs their data cell are
#' @param cell_info a table with one row per query cell-gcm combo, with columns for `gcm`, `tile_no`,
#' `cell_no` and a column for `missing_data`, indicating whether or not the cell is missing data for
#' any variable for that gcm.
#' @param grid_cells an `sf` object with square polygons representing the
#' grid cells. Must contain a `cell_no` column which has
#' the id of each of the cell polygons.
#' @return a png of the xwalk grid cells, symbolized by n_lakes per cell, and the grid tiles
map_missing_cells <- function(out_file, lake_cell_tile_xwalk, cell_info, grid_cells) {
  # Identify which queried grid cells did and did not return data
  grid_cells_w_data <- cell_info %>% filter(missing_data == FALSE) %>% pull(cell_no) %>% unique()
  grid_cells_w_o_data <- cell_info %>% filter(missing_data) %>% pull(cell_no) %>% unique()
  
  # Get unique spatial_ and data_ cell_no pairs
  cell_data_mapping <- lake_cell_tile_xwalk %>%
    dplyr::select(spatial_cell_no, data_cell_no) %>%
    unique()
  
  # Identify which cells that have data are being used for the cells without data
  cells_being_used <- cell_data_mapping %>% filter(spatial_cell_no %in% grid_cells_w_o_data) %>% pull(data_cell_no) %>% unique() 
  
  # Pull cell_nos of queried cells that did not return data that fell inside of the GCM bounding box
  gcm_cells_w_o_data <- cell_data_mapping %>% filter(spatial_cell_no %in% grid_cells_w_o_data) %>% pull(spatial_cell_no) %>% unique()
  
  # Pull cell_nos of queried cells that did not return data that fell outside of the GCM bounding box
  grid_cells_w_o_data_outside_gcm_bbox <- grid_cells_w_o_data[!(grid_cells_w_o_data %in% gcm_cells_w_o_data)]

  # Pull subset of grid cells to map that includes cells without data within the GCM bounding box
  # and the cells used as data for those missing cells
  grid_cells_tomap <- grid_cells %>%
    left_join(cell_data_mapping, by = c("cell_no" = "spatial_cell_no")) %>% 
    filter(cell_no %in% c(gcm_cells_w_o_data, cells_being_used)) %>% 
    mutate(is_missing_data = cell_no %in% grid_cells_w_o_data)
  
  # Get GCM bounding box
  gcm_bbox <- grid_cells %>%
    filter(cell_no %in% grid_cells_w_data) %>%   
    st_bbox() %>%
    st_as_sfc() %>%
    st_as_sf()
 
  # Get spatial information for cells w/o data outside of gcm grid
  grid_cells_w_o_data_outside_gcm_bbox_sf <- grid_cells %>% 
    filter(cell_no %in% grid_cells_w_o_data_outside_gcm_bbox)

  # Limit the map to just the cells we need
  bbox_tomap <- grid_cells %>%
    filter(cell_no %in% c(grid_cells_w_o_data_outside_gcm_bbox, grid_cells_w_data, gcm_cells_w_o_data, cells_being_used)) %>% 
    st_bbox()

  # Generate the map
  missing_cell_plot <- ggplot() +
    geom_sf(data = spData::us_states, fill=NA) +
    geom_sf(data = grid_cells_w_o_data_outside_gcm_bbox_sf, fill='grey80', color='grey60') +
    geom_sf(data = gcm_bbox, fill=NA, color='dodgerblue') +
    geom_sf(data = filter(grid_cells_tomap, is_missing_data), 
            aes(fill = as.character(data_cell_no)), color = NA) +
    geom_sf(data = filter(grid_cells_tomap, !is_missing_data), 
            aes(fill = as.character(data_cell_no), color="Cell has data and is being used\nfor those missing data"), size = 0.5) +
    scico::scale_fill_scico_d(name = sprintf("Cell being used for driver data (n = %s)", length(cells_being_used)), palette = "batlow", direction = -1) +
    scale_color_manual(name = "", values = "red") + 
    coord_sf(xlim = bbox_tomap[c("xmin", "xmax")], ylim = bbox_tomap[c("ymin", "ymax")], expand = TRUE, crs=st_crs(grid_cells)) +
    theme(axis.title.y=element_blank(),
          axis.title.x=element_blank(),
          legend.position="top") + 
    ggtitle("What cells are missing driver data?", 
            subtitle = paste(
              sprintf("%s cells are being used to fill in missing driver data for %s cells", length(cells_being_used), length(gcm_cells_w_o_data)),
              sprintf("%s queried cells (in grey) fell outside of the GCM footprint (blue box)",length(grid_cells_w_o_data_outside_gcm_bbox)),
              sep='\n')) +
    theme_void() +
    theme(legend.position="top")
  
  # save file
  ggsave(out_file, missing_cell_plot, width=10, height=8, dpi=300, bg = "white")
  
  return(out_file)
}
#' @title Convert an sf object into a geoknife::simplegeom, so that
#' it can be used in the geoknife query.
#' @description `geoknife` only works  with `sp` objects but not `sf`
#' objects, so you need to convert these to a data.frame.
#' @param sf_obj spatial features object with at least a `geometry`
#' field and a `cell_no` field. `cell_no` contains the names for each
#' of the polygons used in the query.
sf_pts_to_simplegeom <- function(sf_obj) {
  sf_obj %>%
    st_coordinates() %>%
    t() %>% as.data.frame() %>%
    # Include the cell numbers in the GDP geom so that the
    # results can be linked to the correct cells
    setNames(sf_obj$cell_no) %>%
    simplegeom()
}

#' @title Download GCM data from GDP for each tile and save to a feather file.
#' @description This function accepts each of the query parameters for the
#' geoknife job as an argument. Currently missing the "knife" parameter.
#' The result of the geoknife job is saved as a feather file.
#' @param out_file_template string representing the filepath at which to save
#' the feather file output of the data returned from GDP. The first `%s` is
#' used as the placeholder for the `gcm_name` and the second is for the `tile_no`.
#' @param query_geom an `sf` object that represents the polygons to use to query
#' GDP. Will reproject to EPS = 4326 to query GDP. Expects a column called
#' `tile_no` to represent the current group of grid cells being queried.
#' @param gcm_name name of one of the six GCMs to use to construct the query URL.
#' @param gcm_projection_period name given to the different projection periods. Each
#' GCM dataset contains data for all of the available projection time periods.
#' @param query_vars character vector of the variables that should be downloaded
#' from each of the GCMs. For a list of what variables are available, see
#' https://cida.usgs.gov/thredds/ncss/notaro_GFDL_2040_2059/dataset.html.
#' Note that we can't use `mrso` until https://github.com/USGS-R/geoknife/issues/399
#' is fixed, but we shouldn't need it for the GLM runs that these data feed anyways.
#' @param query_dates character or date vector with two dates: the first is the
#' earliest date in your query and the second is the latest date in your query.
#' @param query_knife the algorithm used to summarize the ouput. Currently `NULL`,
#' which uses `geoknife` defaults.
download_gcm_data <- function(out_file_template, query_geom,
                              gcm_name, gcm_projection_period, query_vars,
                              query_dates, query_knife = NULL) {

  # convert grid cell centroids into geoknife-friendly format
  query_simplegeom <- sf_pts_to_simplegeom(query_geom)

  # Build query_url
  query_url <- sprintf(
    "http://gdp-netcdfdev.cr.usgs.gov:8080/thredds/dodsC/notaro_debias_%s",
    tolower(gcm_name))

  # Stop now if you aren't on VPN
  if(!RCurl::url.exists(paste0(query_url, ".html")))
    stop("You must be connected to USGS VPN to download debiased GCMs")

  # Retry the GDP query multiple times if it doesn't work.
  retry({

    # TODO: temporary fix to be able to download data. Change
    # after https://github.com/USGS-R/geoknife/issues/366 resolved.
    gconfig('sleep.time' = 45, 'wait' = TRUE)

    # construct and submit query
    gcm_job <- geoknife(
      stencil = query_simplegeom,
      fabric = webdata(
        url = query_url,
        variables = query_vars,
        times = query_dates
      )
      # The default knife algorithm is appropriate
    )

    gcm_job_status <- check(gcm_job)$statusType
  },
  # Check the value of the last thing in the expr above (denoted
  # by the ".") to decide if you should retry or not
  until = ~ . == "ProcessSucceeded",
  max_tries = 5)

  my_data <- result(gcm_job, with.units = TRUE)

  # Build out_file name
  out_file <- sprintf(out_file_template,
                      gcm_name,
                      gcm_projection_period,
                      query_geom$tile_no[1])

  # write file
  arrow::write_feather(my_data, out_file)

  return(out_file)
}

#' @title Munge the output from the GDP query, save it and return information
#' about which queried cells are missing data. This function maps over
#' `gcm_data_raw_feather`, so data for a single GCM and a single tile is munged
#' within this function. Each tile contains up to 225 GCM cells.
#' @description This function loads in the raw data returned by GDP, calls
#' `munge_notaro_to_glm()` to munge that data, identifies which cells are
#' missing data for *any* GLM variable by calling `identify_cells_missing_data()`,
#' filters the munged data to only those cells that are not missing data, and saves
#' that data to a file with the exact same name as the in file, except that the
#' "_raw" part of the `in_file` filepath is replaced with "_munged".
#' @param in_file filepath to a feather file containing the hourly geoknife
#' data, named with the the gcm, dates, and tile number as
#' '7_drivers_munge/tmp/7_GCM_{gcm_name}_{projection_period}_tile{tile_no}_raw.feather'
#' @param gcm_names names of the six GCMs
#' @param query_tiles vector of tile ids used in query to GDP
#' @return a list with two elements: 1) `file_out` - the name of the output
#' feather file, and 2) `cell_info` - a tibble with a row for each cell in the
#' tile for which data are being munged, with the columns `gcm` - the gcm for which data is
#' being munged, `tile_no` the tile for which data are being munged, `cell_no` for the cell
#' number, and `missing_data` - a T/F logical indicating whether or not the cell is
#' missing data for *any* variable for the gcm for which data are being munged.
munge_gdp_output <- function(in_file, gcm_names, query_tiles) {
  # Pull gcm name and tile_no
  gcm_name <- str_extract(in_file, paste(gcm_names, collapse="|"))
  tile_no <- str_extract(in_file, paste(query_tiles, collapse="|"))

  # read in raw data returned from GDP
  raw_data <- arrow::read_feather(in_file)

  # munge the raw data
  munged_data <- munge_notaro_to_glm(raw_data)

  # generate a tibble with a row for every cell in the current tile
  # indicating which cells, if any, are missing data for *any* GLM variable
  # for the current GCM
  cell_info <- identify_cells_missing_data(munged_data, gcm_name, tile_no)

  # exclude cells that contain NaN from the munged data
  nan_cells <- filter(cell_info, missing_data==TRUE) %>% pull(cell_no)
  munged_data_excl_nan_cells <- filter(munged_data, !(cell_no %in% nan_cells))

  # save the filtered munged data
  out_file <- gsub("_raw.feather", "_munged.feather", in_file)
  arrow::write_feather(munged_data_excl_nan_cells, out_file)

  # return the out_file name and the cell_info tibble in a list
  return(list(file_out = out_file, cell_info=cell_info))
}

#' @title Identify GCM cells that are missing data
#' @description Figure out which cells, if any, among the munged data for a
#' given gcm and tile are missing data for *any* GLM variable.
#' @param munged_data a tibble of munged GCM data with appropriate units for
#' use with GLM
#' @param gcm_name the name of one of the 6 gcms, for which raw geoknife data
#' were munged
#' @param tile_no the id of the tile for which raw geoknife data were munged
#' @return `cell_info` - a tibble with a row for each cell in the tile for
#' which data are being munged, with the columns `gcm` - the gcm for which
#' data were munged, `tile_no` the tile for which data were munged, `cell_no`
#' for the cell number, and `missing_data` - a T/F logical indicating whether
#' or not the cell is missing data for *any* variable for the gcm for which
#' data were munged.
identify_cells_missing_data <- function(munged_data, gcm_name, tile_no) {
  # Figure out if any cells have NaNs for any variables.
  # The munged_data is in long format, with a column for cell_no.
  # Use 'any_vars' as the second argument to `filter_at()` to catch cells where
  # any variables are missing for a cell [CURRENT APPROACH], or 'all_vars' as
  # the second argument to catch cells where all variables are missing for a cell
  munged_nans <- munged_data %>%
    group_by(cell_no) %>%
    summarize(across(-time, list(n_nan=~sum(is.nan(.))))) %>%
    filter_at(vars(-cell_no), any_vars(. != 0))

  # Create an output tibble documenting which cells are and are not
  # missing data, noting the gcm name and tile_no
  cell_info <- tibble(
    cell_no = unique(munged_data$cell_no)) %>%
    mutate(missing_data = case_when(
      cell_no %in% munged_nans$cell_no ~ TRUE,
      TRUE ~ FALSE)) %>%
    mutate(gcm = gcm_name, tile_no = tile_no, .before=1) %>%
    arrange(cell_no)

  return(cell_info)
}

#' @title Convert Notaro GCM data to GLM-ready data
#' @description The final GCM driver data needs certain column names and units
#' to be used for GLM. This function converts GCM variables into appropriate
#' units.
#' @param raw_data the raw the hourly geoknife data
#' @value a table saved as a feather file with the following columns:
#' `time`: class Date denoting a single day
#' `cell_no`: a numeric value indicating which cell in the grid that the data
#' belongs to (note: the data table only includes queried cells that returned
#' data for all queried variables)
#' `Shortwave`: shortwave radiation (W/m2); a numeric value copied from the Notaro `rsds_debias` variable
#' `Longwave`: longwave radiation (W/m2); a numeric value copied from the Notaro `rsdl_debias` variable
#' `AirTemp`: air temperature (C); a numeric value copied from the Notaro `tas_debias` variable
#' `RelHum`: relative humidity (%); a numeric value copied from the Notaro `rh_debias` variable
#' `WindSpeed`: wind speed (m/s); a numeric value copied from the Notaro `rsdl_debias` variable
#' `Rain`: rate of precipitation as water (m/day); a numeric value converted to m/day from the
#' Notaro variable `prcp_debias` which is in mm/day.
#' `Snow`: the rate of snowfall (m/day); a numeric value derived from the `Rain`
#' column and assumes the snow depth is 10 times the water equivalent (`Rain`) when the
#' temperature (`AirTemp`) is below freezing.
#' @return a tibble of the munged data
munge_notaro_to_glm <- function(raw_data) {

  # This line will fail if the units don't match our assumptions
  validate_notaro_units_assumptions(raw_data)

  # Munge the raw data
  munged_data <- raw_data %>%

    # Create a column with the actual date
    convert_notaro_dates() %>%

    # Pivot to long format first to get cells as a column.
    pivot_longer(cols = -c(date, variable, statistic, units),
                 names_to = "cell_no", values_to = "val") %>%
    mutate(cell_no = as.numeric(cell_no)) %>%

    # Now pivot wider to makes each variable a column for easy, readable munging
    # Also, removes `units` and `statistic` columns which are specific to each variable
    pivot_wider(id_cols = c("date", "cell_no"), names_from = variable, values_from = val) %>%

    # Unit conversions to get GLM-ready variables from GCM ones
    mutate(
      Rain = prcp_debias / 1000 # mm to m
    ) %>%

    # Simply rename GCM variables into GLM variables
    mutate(AirTemp = tas_debias,
           RelHum = rh_debias,
           Shortwave = rsds_debias,
           Longwave = rsdl_debias,
           WindSpeed = windspeed_debias
    ) %>%

    # Calculate GLM variables using other existing variables
    mutate(Snow = ifelse(AirTemp < 0, Rain*10, 0), # When air temp is below freezing, any precip should be considered snow
           Rain = ifelse(AirTemp < 0, 0, Rain)
    ) %>%

    # Catch NA values generated through calculations for Rain and Snow variables
    # should be NaN values to be consistent with other variables and for writing to netCDF
    mutate(Snow = ifelse(is.na(Snow), NaN, Snow),
           Rain = ifelse(is.na(Rain), NaN, Rain)
    ) %>%

    # Keep only the columns we need
    select(time = date,
           cell_no,
           Shortwave,
           Longwave,
           AirTemp,
           RelHum,
           WindSpeed,
           Rain,
           Snow
    )

  return(munged_data)
}

#' @title Check that units for variables downloaded match our assumptions.
#' @description Before the rest of `munge_notaro_to_glm()` can run, we need
#' to make sure that data are returned in the units that the conversion
#' functions are set up to handle. If they have different units OR if there
#' is a new variable not in our list of assumed units, then an error is thrown.
#' @param data_in a data.frame with a least the columns `variable` and `units`
validate_notaro_units_assumptions <- function(data_in) {

  # Check units assumptions
  units_check_out <- data_in %>%
    select(variable, units) %>%
    unique() %>%
    mutate(passes_assumption = case_when(
      variable == "prcp_debias" ~ units == "mm/day",
      variable == "tas_debias" ~ units == "C",
      variable == "rh_debias" ~ units == "%",
      variable == "rsds_debias" ~ units == "W/m2",
      variable == "rsdl_debias" ~ units == "W/m2",
      variable == "windspeed_debias" ~ units == "m/s",
      # For any variable not in our checks, return false
      TRUE ~ FALSE
    ))

  all_passed <- all(units_check_out$passes_assumption)

  # Cause failure if any of these units are different or there is an
  # variable in the dataset that does not appear in this list.
  if(!all_passed) {
    failed_i <- which(!units_check_out$passes_assumption)
    stop_message <- sprintf("The following units do not match the assumptions: %s",
                            paste(units_check_out$variable[failed_i], collapse = ", "))
    stop(stop_message)
  }
}

#' @title Create correct dates for downscaled Notaro GCM data
#' @description Called within `munge_notaro_to_glm()`to correctly use the
#' `DateTime` and `time(day of year)` columns to create a date. The reason this
#' is more complex than simple addition of this two fields is because 1) the
#' downscaled GCM data assumes there are no leap days, and 2) the dates returned
#' by GDP in `DateTime` do not always reflect the appropriate start day for the
#' current year of data, e.g. the `DateTime` values for 1985 data are `1984-12-31 23:15:03`.
#' @param data_in a data.frame with a least the columns `DateTime` (a POSIXct
#' object with the first day of each year of data), `time(day of year)` (a numeric
#' column with the day of year, ranging from 0:364), and `variable` (the downscaled
#' GCM column containing data variables, e.g. `prcp_debias`).
#' @value a data.frame with the columns `date`, `variable`, and any other column from
#' `data_in` that is not `DateTime` or `time(day of year)`.
convert_notaro_dates <- function(data_in) {
  data_in %>%

    ### Group data into the different big time chunks by finding and identifying ###

    # This groups data at the points where 2000 is right before 2040, and 2040 is right before 2060
    mutate(diff_days = abs(as.numeric(DateTime - lag(DateTime))/86400)) %>%
    # Use a difference in days greater than one year to determine if a row is the
    # start of a new time period
    mutate(new_time_period = is.na(diff_days) | diff_days > 366) %>%
    # Each new time period ticks the counter up by 1
    mutate(new_time_period_i = cumsum(new_time_period)) %>%

    ### For each time period + variable group, identify the earliest year in the period and use that to create a column of years ###

    # Not able to just to `format(DateTime, "%Y")` to get the year because not all have a `DateTime` in the
    # correct year, e.g. 1985 records have a DateTime of `1984-12-31 23:15:03`.

    # First treat rows for the same variable and time period as a group
    group_by(variable, new_time_period_i) %>%
    # Determine the earliest year in the time period
    mutate(earliest_year = as.numeric(format(min(DateTime), "%Y"))) %>%
    # Create groups of the years (`time(day of year)` will cycle through 0:364 for each
    # year, so tick a counter up for every 0 in the column)
    mutate(year_n = cumsum(`time(day of year)` == 0) - 1) %>%
    mutate(year_i = earliest_year + year_n) %>%
    ungroup() %>%

    ### For each year + variable group, correctly convert to the DateTime field to a date ###

    # The GCMs assume there are no leap years, but R automatically assumes leap years, so we need
    # to implement a work around for that.

    group_by(variable, new_time_period_i, year_i) %>%
    # Start by creating a date and allow leap days to be added
    mutate(date_initial = as.Date(sprintf("%s-01-01", year_i)) + `time(day of year)`) %>%
    # If the date in the column is a leap day (Feb 29), the dates need to be adjusted by 1 day
    # meaning the 59th day should be Mar 1 not Feb 29.
    mutate(leap_adjustment = if_else(format(date_initial, "%m-%d") == "02-29", 1, 0)) %>%
    # Use cumsum to make sure the leap_adjustment is used for every subsequent day of the year following a leap day
    mutate(total_days_to_add = cumsum(leap_adjustment)) %>%
    ungroup() %>% # Reset groups since they are no longer needed
    # Using the initial date created, adjust the days to shift anything in a leap year that follows Feb 29
    mutate(date_corrected = date_initial + total_days_to_add) %>%

    # Keep the variable, corrected date, statistic, and units columns, as well as, any column
    # whose name only contains numbers (since those should represent all the grid cell's data)
    select(date = date_corrected, matches("[0-9]+"), variable, statistic, units)
}
