library(targets)

options(tidyverse.quiet = TRUE)
tar_option_set(packages = c(
  "tidyverse",
  "sf",
  "ncdf4",
  "scipiper",
  "ggplot2",
  "geoknife",
  "arrow"
))

source('7_drivers_munge/src/GCM_driver_utils.R')

targets_list <- list(

  # Hard code GCM grid parameters
  tar_target(grid_params, tibble(
    crs = "+proj=lcc +lat_0=45 + lon_0=-97 +lat_1=36 +lat_2=52 +x_0=0 +y_0=0 +ellps=WGS84 +units=m",
    grid_size_m = 25000,
    x_min = -200000,
    y_min = -1125000,
    n_cell_x = 110,
    n_cell_y = 85
  )),

  # Reconstruct GCM grid using grid parameters from GDP defined above
  tar_target(grid_cells_sf, reconstruct_gcm_grid(grid_params)),

  # Get centroids of grid cells
  tar_target(grid_cell_centroids_sf, sf::st_centroid(grid_cells_sf)),

  # Group cells into tiles to use for querying GDP. Constructing tiles
  # to be made of 100 cells in a 10x10 grid.
  tar_target(grid_tiles_sf, construct_grid_tiles(grid_params, tile_dim=10)),

  # Load and prepare lake centroids for matching to the GCM grid using
  # the `centroid_lakes_sf.rds` file from our `scipiper` pipeline
  tar_target(lake_centroids_sf_rds, gd_get('2_crosswalk_munge/out/centroid_lakes_sf.rds.ind'), format='file'),
  tar_target(query_lake_centroids_sf,
             readRDS(lake_centroids_sf_rds) %>%
               # Subset to 5 lakes for now for testing
               subset_lake_centroids() %>%
               # Lake centroids should be in the same CRS as the GCM grid
               sf::st_transform(grid_params$crs)
  ),

  # Get cells associated with each tile
  # MAPPING over grid tiles
  tar_target(
    tile_cells_sf,
    get_tile_cells(grid_cell_centroids_sf, grid_cells_sf, grid_tiles_sf),
    pattern = map(grid_tiles_sf)
  ),

  # Spatially filter cells within each tile to only those cells
  # that contain lake centroids
  tar_target(
    query_cells_sf,
    get_query_cells(tile_cells_sf, query_lake_centroids_sf),
    pattern = map(tile_cells_sf)
  ),

  # Get mapping of which lakes are in which cells (w/ no spatial info)
  # (for use later so that we know what data to pull for each lake)
  tar_target(
    lake_cell_xwalk,
    get_lake_cell_xwalk(query_lake_centroids_sf, tile_cells_sf),
    pattern = map(tile_cells_sf)
  ),

  # Spatially filter cell centroids to those that fall within
  # tile cells that have lakes (since we want to use cell
  # centroids in query, not cell polygons)
  tar_target(
    query_cell_centroids_sf,
    get_query_centroids(query_cells_sf, grid_cell_centroids_sf),
    pattern = map(query_cells_sf)
  ),

  # MAP QUERY
  # For now, mapping over tiles, and saving empty file if
  # tiles do not contain any cells w/ lakes
  # alternatively, can manually slice using branch indices
  # pattern = slice(query_cells_sf, index = c(37,47,48)
  # Get get ids of tiles with lakes with unique(lake_cell_xwalk$tile_no)
  # matches branch indices. Unfortunately seems like you can't use
  # a predefined vector to supply the indices to slice()
  tar_target(
    query_map_png,
    map_query(
      out_file_template = '7_drivers_munge/out/query_map_tileXX.png',
      lake_centroids = query_lake_centroids_sf,
      grid_tiles = grid_tiles_sf,
      grid_cells = grid_cells_sf,
      cells_w_lakes = query_cells_sf
    ),
    pattern = map(query_cells_sf),
    format='file'
  ),

  # BUILD QUERY
  # Define list of GCMs
  tar_target(
    gcm_names,
    c('ACCESS', 'GFDL', 'CNRM', 'IPSL', 'MRI', 'MIROC')
  ),
  # TODO: Address issue that not all tiles contain cells that contain lakes
  # so don't need to pull for each tile, so therefore don't need to generate file
  # in each branch. For now, writing empty file, but means final vector
  # contains placeholder filename for any tile that doesn't contain cells w/ lakes.
  # Ideally, would only return filename if data is downloaded, but not sure
  # targets would accept that for a file target.
  tar_target(
    gcm_data_raw_feather,
    download_gcm_data(
      out_file_template = "7_drivers_munge/tmp/7_GCM_GCMname_tileXX_raw.feather",
      query_geom = query_cell_centroids_sf,
      query_url_template = "https://cida.usgs.gov/thredds/dodsC/notaro_GCMname_1980_1999",
      gcm_name = gcm_names,
      # Data definitions: https://cida.usgs.gov/thredds/ncss/notaro_GFDL_2040_2059/dataset.html
      # Can't use `mrso` until https://github.com/USGS-R/geoknife/issues/399 is fixed, but we shouldn't need it
      query_vars = c("evspsbl", "hfss"),
      query_dates = c('1999-01-01', '1999-01-15')
    ),
    pattern = cross(query_cell_centroids_sf, gcm_names),
    format = "file"
  ),
  # TODO - munge data
  #
  # Get list of final output files
  tar_target(
    gcm_files_out,
    c(gcm_data_raw_feather, query_map_png)
  )
)

# Return the complete list of targets
c(targets_list)
