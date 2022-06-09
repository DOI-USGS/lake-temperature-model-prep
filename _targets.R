library(targets)

options(tidyverse.quiet = TRUE)
tar_option_set(packages = c(
  "tidyverse",
  "sf",
  "ncdf4",
  "ncdfgeom", # You need >= v1.1.2
  "RNetCDF",
  "scipiper",
  "ggplot2",
  "scico",
  "geoknife", # You need >= v1.6.6
  "arrow",
  "retry"
))

source('7_drivers_munge/src/GCM_driver_utils.R')
source('7_drivers_munge/src/GCM_driver_nc_utils.R')

targets_list <- list(

  ##### Construct overall GCM grids #####

  # Hard code GCM grid parameters
  tar_target(grid_params, tibble(
    crs = "+proj=lcc +lat_0=45 + lon_0=-97 +lat_1=36 +lat_2=52 +x_0=0 +y_0=0 +ellps=WGS84 +units=m",
    cellsize = 25000,
    xmin = -2700000, # Centroid of bottom left cell
    ymin = -1750000, # Centroid of bottom left cell
    nx = 217,
    ny = 141
  )),

  # Create larger tiles to use for querying GDP with groups of cells.
  # Constructing tiles to be made of 400 cells in a 20x20 grid. Attempts
  # at 23x23 and bigger resulted in timeouts.
  tar_target(grid_tiles_sf, construct_grid_tiles(grid_params, tile_dim=20)),

  # Reconstruct GCM grid using grid parameters from GDP defined above
  tar_target(grid_cells_sf, reconstruct_gcm_grid(grid_params)),
  tar_target(grid_cell_centroids_sf, sf::st_centroid(grid_cells_sf)),

  ##### Prepare lake centroids for matching to grid #####

  # Identify the lakes that we can model with GLM first
  tar_target(nml_rds, gd_get('7_config_merge/out/nml_list.rds.ind'), format='file'),
  tar_target(nml_site_ids, names(readRDS(nml_rds))),
  
  # Load and prepare lake centroids for matching to the GCM grid using
  # the `centroid_lakes_sf.rds` file from our `scipiper` pipeline
  tar_target(lake_centroids_sf_rds, gd_get('2_crosswalk_munge/out/centroid_lakes_sf.rds.ind'), format='file'),
  tar_target(lake_to_state_xwalk_rds, gd_get('2_crosswalk_munge/out/lake_to_state_xwalk.rds.ind'), format='file'),
  tar_target(query_lake_centroids_sf,
             readRDS(lake_centroids_sf_rds) %>%
               left_join(readRDS(lake_to_state_xwalk_rds), by = "site_id") %>%
               # Subset to just lakes with NMLs, so that we aren't wasting
               # time getting driver data for cells with lakes that can't
               # be modeled using GLM anyways.
               filter(site_id %in% nml_site_ids) %>%
               # Lake centroids should be in the same CRS as the GCM grid
               sf::st_transform(grid_params$crs)
  ),

  ##### Create crosswalks between cells, tiles, and lakes #####

  # Get mapping of which cells are in which tiles (as a non-spatial data.frame)
  tar_target(cell_tile_xwalk_df, get_cell_tile_xwalk(grid_cell_centroids_sf, grid_tiles_sf)),

  # Get mapping of which lakes are in which cells and tiles, based on a spatial join
  # (as a non-spatial data.frame). Not currently being used here, except for mapping the query.
  tar_target(lake_cell_tile_spatial_xwalk_df,
             get_lake_cell_tile_spatial_xwalk(query_lake_centroids_sf, grid_cells_sf, cell_tile_xwalk_df)),

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

  # Reproject query_cell_centroids to WGS84
  tar_target(query_cell_centroids_sf_WGS84,
             sf::st_transform(query_cell_centroids_sf, crs = 4326)),

  # Split query cells by tile_no to map over
  tar_target(query_tiles, unique(query_cell_centroids_sf_WGS84$tile_no)),
  tar_target(query_cells_centroids_list_by_tile,
             dplyr::filter(query_cell_centroids_sf_WGS84, tile_no == query_tiles),
             pattern = map(query_tiles),
             iteration = "list"),

  ##### Create an image showing the full query, with n_lakes per cell #####
  
  tar_target(
    query_tile_cell_map_png,
    map_tiles_cells(
      out_file = '7_drivers_munge/out/query_tile_cell_map.png',
      lake_cell_tile_xwalk = lake_cell_tile_spatial_xwalk_df,
      query_tiles = query_tiles,
      query_cells = query_cells,
      grid_tiles = grid_tiles_sf,
      grid_cells = grid_cells_sf
    ),
    format='file'
  ),

  ##### Download data from the GDP #####

  # BUILD QUERY
  # Define list of GCMs
  tar_target(gcm_names, c('ACCESS', 'GFDL', 'CNRM', 'IPSL', 'MRI', 'MIROC5')),
  tar_target(gcm_dates_df,
             tibble(
               projection_period = c('1980_2099'),
               start_datetime = c('1980-01-01 00:00:00'),
               # Include midnight on the final day of the time period
               end_datetime = c('2099-12-31 11:59:59')
             )),

  # Notaro variable definitions (see http://gdp-netcdfdev.cr.usgs.gov:8080/thredds/dodsC/notaro_debias_mri.html)
  tar_target(gcm_query_vars, c(
    "prcp_debias", # Precipitation (mm/day)
    "tas_debias", # Air temp (deg C)
    "rh_debias", # Relative humidity (%)
    "rsds_debias", # Surface shortwave radiation (W/m2)
    "rsdl_debias", # Surface longwave radiation (W/m2)
    "windspeed_debias" # Wind speed (m/s)
    )),

  # Download data from GDP for each tile, GCM name, and GCM projection period combination.
  # If the cells in a tile don't change, then the tile should not need to rebuild.
  tar_target(
    gcm_data_raw_feather,
    download_gcm_data(
      out_file_template = "7_drivers_munge/tmp/7_GCM_%s_%s_tile%s_raw.feather",
      query_geom = query_cells_centroids_list_by_tile,
      gcm_name = gcm_names,
      gcm_projection_period = gcm_dates_df$projection_period,
      query_vars = gcm_query_vars,
      query_dates = c(gcm_dates_df$start_datetime, gcm_dates_df$end_datetime)
    ),
    pattern = cross(query_cells_centroids_list_by_tile, gcm_names, gcm_dates_df),
    format = "file",
    error = "continue"
  ),

  ##### Munge GDP output into NetCDF files that will feed into GLM #####

  # Munge GCM variables into useable GLM variables and correct units
  # For each tile-gcm combo this function returns a list with 2 elements:
  # 1) file_out - the name of the munged output file, which contains data
  # only for queried cells in that tile that returned data for all variables, and
  # 2) cell_info - a tibble with a row for each queried cell in that tile, indicating
  # whether or not that cell is missing data for *any* variable for that gcm
  # Note: we are munging and saving the data & identifying missing cells in a single
  # target (rather than having a munging target and then a target to write the munged
  # data and another to identify cells with missing data) to avoid the I/O overhead of
  # returning the munged data as a dataframe, as targets would be writing a file
  # beneath the surface to store that object target.
  tar_target(
    glm_ready_gcm_data_list,
    munge_gdp_output(gcm_data_raw_feather, gcm_names, query_tiles),
    pattern = map(gcm_data_raw_feather)
  ),

  # Combine the `file_out` elements of the gcm_ready_gcm_data_list branches
  # into a single vector of file targets
  tar_target(
    glm_ready_gcm_data_feather,
    glm_ready_gcm_data_list$file_out,
    pattern = map(glm_ready_gcm_data_list),
    format = "file"
  ),

  # Combine the `cell_info` elements of the gcm_ready_gcm_data_list branches
  # into a single tibble of cell information, with a row per cell-gcm combo
  tar_target(
    glm_ready_gcm_data_cell_info,
    bind_rows(glm_ready_gcm_data_list[grepl("cell_info", names(glm_ready_gcm_data_list))])
  ),

  # Re-do mapping of which lakes are in which cells and tiles, using only the query
  # cells that returned data and did not have missing data for any variables for any GCMs.
  # Here, the pool of cells to which lakes can be matched are those cells that returned data,
  # to ensure that lakes that fell within cells that were missing data are re-assigned to
  # a cell that returned data. Lakes are preferentially matched to cells within the same
  # grid row and within `x_buffer` columns from the cell that the lake falls within.
  # If no cells that returned data meet that criteria, the lake is matched to the closest
  # cell that did return data, regardless of row. This xwalk is being used in
  # `lake-temperature-process-models` to determine what meteorological data to pull for each lake.
  tar_target(lake_cell_tile_xwalk_df,
             adjust_lake_cell_tile_xwalk(lake_cell_tile_spatial_xwalk_df, query_lake_centroids_sf,
                                         query_cell_centroids_sf, glm_ready_gcm_data_cell_info, x_buffer=3)
             ),
  
  ##### Create an image showing missing cells from the query #####
  tar_target(missing_cells, glm_ready_gcm_data_cell_info %>% filter(missing_data) %>% pull(cell_no) %>% unique()),
  tar_target(
    query_tile_cell_map_missing_png,
    map_missing_cells(
      out_file = '7_drivers_munge/out/query_tile_cell_map_missing.png',
      lake_cell_tile_xwalk = lake_cell_tile_xwalk_df,
      missing_cells = missing_cells,
      grid_cells = grid_cells_sf
    ),
    format='file'
  ),

  # Save the revised lake-cell-tile mapping for use in `lake-temperature-process-models`
  tar_target(lake_cell_tile_xwalk_csv, {
    out_file <- "7_drivers_munge/out/lake_cell_tile_xwalk.csv"
    write_csv(lake_cell_tile_xwalk_df, out_file)
    return(out_file)
  }, format = 'file'),

  # Group daily feather files by GCM to map over and include
  # file hashes to trigger rebuilds for groups as needed
  tar_target(gcm_data_daily_feather_group_by_gcm,
             tibble(
               gcm_file = glm_ready_gcm_data_feather,
               data = tools::md5sum(gcm_file),
               gcm_name = str_extract(gcm_file, paste(gcm_names, collapse="|"))
             ) %>%
               group_by(gcm_name) %>%
               tar_group(),
             iteration = "group"),

  # Setup table of GLM variable definitions to use in NetCDF file metadata
  tar_target(glm_vars_info,
             tibble(
               var_name = c("Rain", "Snow", "AirTemp", "RelHum", "Shortwave", "Longwave", "WindSpeed"),
               longname = c("Total daily rainfall",
                            "Total daily snowfall derived from precipitation and air temp",
                            "Average daily near surface air temperature",
                            "Average daily percent relative humidity",
                            "Average daily surface downward shortwave flux in air",
                            "Average daily surface downward longwave flux in air",
                            "Average daily windspeed derived from anemometric zonal and anenometric meridional wind components"),
               units = c("m/day", "m/day", "degrees Celcius", "percent", "W/m2", "W/m2", "m/s"),
               data_precision = "float",
               compression_precision = c('.3','.3','.2','.1','.1','.1','.3' )
             )),

  # Create single NetCDF files for each of the GCMs
  tar_target(
    gcm_nc, {
      # Grouped by GCM name so this should just return one unique value
      gcm_name <- unique(gcm_data_daily_feather_group_by_gcm$gcm_name)

      generate_gcm_nc(
        nc_file = sprintf("7_drivers_munge/out/GCM_%s.nc", gcm_name),
        gcm_raw_files = gcm_data_daily_feather_group_by_gcm$gcm_file,
        vars_info = glm_vars_info,
        grid_params = grid_params,
        spatial_info = query_cell_centroids_sf_WGS84,
        gcm_name = gcm_name,
        compression = FALSE
      )
    },
    pattern = map(gcm_data_daily_feather_group_by_gcm),
    format = 'file',
    error = 'continue'
  ),


  ##### Get list of final output files to link back to scipiper pipeline #####
  tar_target(
    gcm_files_out,
    c(gcm_nc, lake_cell_tile_xwalk_csv, query_tile_cell_map_png, query_tile_cell_map_missing_png),
    format = 'file'
  )
)

# Return the complete list of targets
c(targets_list)
