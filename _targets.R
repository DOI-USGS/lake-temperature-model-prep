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
source('7_drivers_munge/src/GCM_driver_nc_utils.R')

targets_list <- list(

  ##### Construct overall GCM grids #####

  # Hard code GCM grid parameters
  tar_target(grid_params, tibble(
    crs = "+proj=lcc +lat_0=45 + lon_0=-97 +lat_1=36 +lat_2=52 +x_0=0 +y_0=0 +ellps=WGS84 +units=m",
    cellsize = 25000,
    xmin = -200000,
    ymin = -1125000,
    nx = 110,
    ny = 85
  )),

  # Create larger tiles to use for querying GDP with groups of cells.
  # Constructing tiles to be made of 100 cells in a 10x10 grid.
  # TODO: this can be much larger! JR thinks GDP can handle ~1000 cells
  # at a time. Since marching through timesteps on GDP is slower than
  # scaling out spatially, it would make sense to do queries with as big
  # of a spatial resolution as GDP will handle.
  tar_target(grid_tiles_sf, construct_grid_tiles(grid_params, tile_dim=10)),

  # Reconstruct GCM grid using grid parameters from GDP defined above
  tar_target(grid_cells_sf, reconstruct_gcm_grid(grid_params)),
  tar_target(grid_cell_centroids_sf, sf::st_centroid(grid_cells_sf)),

  ##### Prepare lake centroids for matching to grid #####

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

  ##### Create crosswalks between cells, tiles, and lakes #####

  # Get mapping of which cells are in which tiles (w/ no spatial info)
  tar_target(cell_tile_xwalk_df, get_cell_tile_xwalk(grid_cell_centroids_sf, grid_tiles_sf)),

  # Get mapping of which lakes are in which cells (w/ no spatial info). Not currently
  # being used, but this info saved as a file could be useful in the modeling part so
  # that we know what data to pull for each lake).
  tar_target(lake_cell_xwalk_df, get_lake_cell_xwalk(query_lake_centroids_sf, grid_cells_sf)),

  ##### Prepare grid cell centroids for querying GDP #####
  # Don't need to do everything, only need to do cells where we have lakes. Group query
  # to GDP by 10x10 tiles so that we have manageable sized geoknife calls.

  # Spatially filter cells to only those cells that contain lake centroids and
  # share how many lakes are in each for summarization purposes. Make a second target
  # that is only the cell number so that adding lakes in the same cell won't kick
  # off any rebuilds.
  tar_target(query_cells_info_df, get_query_cells(grid_cells_sf, query_lake_centroids_sf)),
  tar_target(query_cells, query_cells_info_df$cell_no),

  # Get the centroids for the cells that need to be queried (since we need to use cell
  # centroids in the GDP query, not cell polygons) and m
  tar_target(query_cell_centroids_sf,
             grid_cell_centroids_sf %>%
               dplyr::filter(cell_no %in% query_cells) %>%
               dplyr::left_join(cell_tile_xwalk_df, by = "cell_no")
  ),

  # Split query cells by tile_no to map over
  tar_target(query_tiles, unique(query_cell_centroids_sf$tile_no)),
  tar_target(query_cells_centroids_list_by_tile,
             dplyr::filter(query_cell_centroids_sf, tile_no == query_tiles),
             pattern = map(query_tiles),
             iteration = "list"),

  ##### Create an image showing what each query will contain #####
  # TODO: maybe remove this once we are happy with this process

  # Save image of each map query for exploratory purposes
  tar_target(
    query_map_png,
    map_query(
      out_file_template = '7_drivers_munge/out/query_map_tile%s.png',
      lake_centroids = query_lake_centroids_sf,
      grid_tiles = grid_tiles_sf,
      grid_cells = grid_cells_sf,
      cells_w_lakes = query_cells_centroids_list_by_tile
    ),
    pattern = map(query_cells_centroids_list_by_tile),
    iteration = "list",
    format='file'
  ),

  ##### Download data from the GDP #####

  # BUILD QUERY
  # Define list of GCMs
  tar_target(gcm_names, c('ACCESS', 'GFDL')),#, 'CNRM', 'IPSL', 'MRI', 'MIROC')),
  tar_target(gcm_dates, c('1999-01-01 00:00:00', '1999-01-15 23:00:00')),
  tar_target(gcm_vars_info,
             # Gathered manually from https://cida.usgs.gov/thredds/ncss/notaro_GFDL_2040_2059/dataset.html
             tibble(
               var_name = c("evspsbl", "hfss"),#, "pr", "ps", "qas"),
               longname = c("Total evapotranspiration flux", "Sensible heat flux"),
               units = c("(kg * m^2)/s", "W / m^2"),
               precision = "float"
             )),
  tar_target(gcm_vars, gcm_vars_info$var_name),

  # Download data from GDP for each tile & GCM name combination.
  # If the cells in a tile don't change, then the tile should not need to rebuild.
  # TODO: map over other time periods- 1980-1999 + 2040-2059 + 2080-2099
  tar_target(
    gcm_data_raw_feather,
    download_gcm_data(
      out_file_template = "7_drivers_munge/tmp/7_GCM_%s_1980_1999_tile%s_raw.feather",
      query_geom = query_cells_centroids_list_by_tile,
      gcm_name = gcm_names,
      query_vars = gcm_vars,
      query_dates = gcm_dates
    ),
    # TODO: might need to split across variables, too. Once we scale up, our queries
    # might be too large and chunking by variable could help.
    pattern = cross(query_cells_centroids_list_by_tile, gcm_names),
    format = "file"
  ),

  ##### Munge GDP output into NetCDF files that will feed into GLM #####

  # First, need to summarize the hourly data into daily output
  tar_target(
    gcm_data_daily_feather,
    convert_to_daily(gcm_data_raw_feather),
    pattern = map(gcm_data_raw_feather),
    format = "file"
  ),

  # TODO: create snow from rain, see https://github.com/USGS-R/lake-temperature-model-prep/issues/222

  # Group daily feather files by GCM to map over
  # TODO: map over other time periods- 1980-1999 + 2040-2059 + 2080-2099
  tar_target(gcm_data_daily_feather_group_by_gcm,
             tibble(gcm_file = gcm_data_daily_feather) %>%
               mutate(gcm_name = gsub("7_drivers_munge/tmp/7_GCM_|_1980_1999_tile([0-9]+)_daily.feather", "", gcm_file)) %>%
               group_by(gcm_name) %>%
               tar_group(),
             iteration = "group"),

  # Create single NetCDF files for each of the GCMs
  # TODO: map over other time periods- 1980-1999 + 2040-2059 + 2080-2099
  tar_target(
    gcm_nc, {
      gcm_name <- unique(gcm_data_daily_feather_group_by_gcm$gcm_name)
      generate_gcm_nc(
        nc_file = sprintf("7_drivers_munge/out/7_GCM_%s_1980_1999.nc", gcm_name),
        gcm_raw_files = gcm_data_daily_feather_group_by_gcm$gcm_file,
        dim_time_input = seq(as.Date(gcm_dates[1]), as.Date(gcm_dates[2]), by = 1),
        dim_cell_input = grid_cells_sf$cell_no,
        vars_info = gcm_vars_info,
        global_att = sprintf("GCM Notaro %s 1980_1999", gcm_name)
      )
    },
    pattern = map(gcm_data_daily_feather_group_by_gcm)
  ),


  ##### Get list of final output files to link back to scipiper pipeline #####
  tar_target(
    gcm_files_out,
    c(gcm_nc, query_map_png)
  )
)

# Return the complete list of targets
c(targets_list)
