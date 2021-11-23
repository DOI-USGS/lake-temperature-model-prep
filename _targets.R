library(targets)

options(tidyverse.quiet = TRUE)
tar_option_set(packages = c(
  "tidyverse",
  "sf",
  "rgdal",
  "scipiper",
  "ggplot2",
  "geoknife",
  "arrow"
))

source('7_drivers_munge/src/GCM_driver_utils.R')

targets_list <- list(
  # The input from our `scipiper` pipeline
  tar_target(
    lake_centroids_sf_rds,
    gd_get('2_crosswalk_munge/out/centroid_lakes_sf.rds.ind'),
    format='file'
  ),

  # FOR NOW, FOR TESTING - get subset of lake centroids
  tar_target(
    subset_lake_centroids_sf,
    split_lake_centroids(lake_centroids_sf_rds)
  ),

  # TODO: bring in the python workflow to create this shapefile
  # of the reconstructed grid cells
  # Load in the reconstructed grid shapefile as a file target
  tar_target(
    grid_cells_shp,
    '7_drivers_munge/in/gcm_grid_cells.shp',
    format='file'
  ),

  # load the grid cell shapefile using readOGR
  # (so that we get the custom LCC projection information)
  tar_target(
    grid_cells,
    rgdal::readOGR(grid_cells_shp)
  ),

  # Load grid cells shapefile again as sf object
  # using projection of grid cells OGR object
  tar_target(
    grid_cells_sf,
    sf::st_read(grid_cells_shp, layer='gcm_grid_cells') %>%
      sf::st_transform(grid_cells, crs = sf::st_crs(grid_cells))
  ),

  # TODO: bring in the python workflow to create this shapefile
  # of the grid cell centroids
  # Load in the grid cell centroids as a file target
  tar_target(
    grid_cell_centroids_shp,
    '7_drivers_munge/in/gcm_grid_cell_centroids.shp',
    format='file'
  ),

  # load the tiles shapefile using sf
  # using projection of grid cells OGR object
  tar_target(
    grid_cell_centroids_sf,
    sf::st_read(grid_cell_centroids_shp, layer='gcm_grid_cell_centroids') %>%
      sf::st_transform(grid_cells, crs = sf::st_crs(grid_cells))
  ),

  # TODO: bring in the python workflow to create this shapefile
  # of the tiles
  # Load in the shapefile of the grid tiles as a file target
  tar_target(
    grid_tiles_shp,
    '7_drivers_munge/in/gcm_grid_tiles.shp',
    format='file'
  ),

  # load the tiles shapefile using sf
  # using projection of grid cells OGR object
  tar_target(
    grid_tiles_sf,
    sf::st_read(grid_tiles_shp, layer='gcm_grid_tiles') %>%
      sf::st_transform(grid_cells, crs = sf::st_crs(grid_cells))
  ),

  # reproject lake centroids to crs of grid cells
  tar_target(
    query_lake_centroids_sf,
    project_to_grid_crs(subset_lake_centroids_sf, grid_cells)
  ),

  # Pull the tile ids, for mapping
  tar_target(
    grid_tile_ids,
    grid_tiles_sf$tile_no
  ),

  # Get cells associated with each tile
  # MAPPING over grid tile ids
  tar_target(
    tile_cells_sf,
    get_tile_cells(grid_cell_centroids_sf, grid_cells_sf, grid_tiles_sf, grid_tile_ids),
    pattern = map(grid_tile_ids),
    iteration = 'list'
  ),

  # Get mapping of which lakes are in which cells (w/ no spatial info)
  tar_target(
    lake_cell_xwalk,
    get_lake_cell_xwalk(query_lake_centroids_sf, tile_cells_sf),
    pattern = map(tile_cells_sf),
    iteration = 'list'
  ),

  # Spatially filter cells within each tile to only those cells
  # that contain lake centroids
  tar_target(
    query_cells_sf,
    keep_cells_with_lakes(tile_cells_sf, query_lake_centroids_sf),
    pattern = map(tile_cells_sf),
    iteration = 'list'
  ),

  # Spatially filter centroids to those that fall within
  # tile cells that have lakes
  tar_target(
    query_cell_centroids_sf,
    get_query_centroids(query_cells_sf, grid_cell_centroids_sf),
    pattern = map(query_cells_sf),
    iteration = 'list'
  ),

  # Convert the query grid cell centroids into a geoknife-ready format
  # TODO - what projection does the request need to be in?
  # returned proj4string from simplegeom is incorrect
  # may need to reproject query cell centroids to EPSG 4326 first?
  tar_target(
    query_centroids_geoknife,
    sf_pts_to_simplegeom(query_cell_centroids_sf),
    pattern = map(query_cell_centroids_sf),
    iteration = 'list'
  ),

  # TODO: make this parameterized/branched. Right now, just doing
  # a single GCM, but will want to parameterize to each of them +
  # handle chunks of lakes. See this config file example for an example:
  # https://github.com/USGS-R/necsc-lake-modeling/blob/a81a0e7ed6ede66253f765b42568a4d39a5dccc2/configs/ACCESS_config.yml
  tar_target(
    gcm_data_raw_feather,
    download_gcm_data(
      out_file = "7_drivers_munge/tmp/7_GCM_GFDL_raw.feather",
      query_geom = query_centroids_geoknife,
      query_url = "https://cida.usgs.gov/thredds/dodsC/notaro_GFDL_1980_1999",
      # Data definitions: https://cida.usgs.gov/thredds/ncss/notaro_GFDL_2040_2059/dataset.html
      # Can't use `mrso` until https://github.com/USGS-R/geoknife/issues/399 is fixed, but we shouldn't need it
      query_vars = c("evspsbl", "hfss"),
      query_dates = c('1999-01-01', '1999-01-15')
    ),
    format = "file"
  ),

  tar_target(
    query_map_png,
    map_query(
      out_file = '7_drivers_munge/out/query_map.png',
      centroids_sf = query_lake_centroids_sf
    ),
    format='file'
  ),
  tar_target(
    gcm_files_out,
    c(gcm_data_raw_txt, query_map_png)
  )
)

# Return the complete list of targets
c(targets_list)
