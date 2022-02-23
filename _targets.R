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
  "geoknife",
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
  # Constructing tiles to be made of 225 cells in a 15x15 grid.
  # TODO: this can be much larger! JR thinks GDP can handle ~1000 cells
  # at a time. Since marching through timesteps on GDP is slower than
  # scaling out spatially, it would make sense to do queries with as big
  # of a spatial resolution as GDP will handle.
  tar_target(grid_tiles_sf, construct_grid_tiles(grid_params, tile_dim=15)),

  # Reconstruct GCM grid using grid parameters from GDP defined above
  tar_target(grid_cells_sf, reconstruct_gcm_grid(grid_params)),
  tar_target(grid_cell_centroids_sf, sf::st_centroid(grid_cells_sf)),

  ##### Prepare lake centroids for matching to grid #####

  # Load and prepare lake centroids for matching to the GCM grid using
  # the `centroid_lakes_sf.rds` file from our `scipiper` pipeline
  tar_target(lake_centroids_sf_rds, gd_get('2_crosswalk_munge/out/centroid_lakes_sf.rds.ind'), format='file'),
  tar_target(lake_to_state_xwalk_rds, gd_get('2_crosswalk_munge/out/lake_to_state_xwalk.rds.ind'), format='file'),
  tar_target(query_lake_centroids_sf,
             readRDS(lake_centroids_sf_rds) %>%
               left_join(readRDS(lake_to_state_xwalk_rds), by = "site_id") %>%
               # Subset to just MN for now
               filter(state == "MN") %>%
               # Lake centroids should be in the same CRS as the GCM grid
               sf::st_transform(grid_params$crs)
  ),

  ##### Create crosswalks between cells, tiles, and lakes #####

  # Get mapping of which cells are in which tiles (as a non-spatial data.frame)
  tar_target(cell_tile_xwalk_df, get_cell_tile_xwalk(grid_cell_centroids_sf, grid_tiles_sf)),

  # Get mapping of which lakes are in which cells and tiles, based on a spatial join
  # (as a non-spatial data.frame). Not currently being used here, except for mapping the query.
  tar_target(lake_cell_tile_xwalk_spatial_df,
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
      lake_cell_tile_xwalk = lake_cell_tile_xwalk_spatial_df,
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
    # TODO: might need to split across variables, too. Once we scale up, our queries
    # might be too large and chunking by variable could help.
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
  tar_target(
    glm_ready_gcm_data_list,
    munge_notaro_to_glm(gcm_data_raw_feather, gcm_names, unique(query_cells_centroids_list_by_tile$tile_no)),
    pattern = map(gcm_data_raw_feather, cross(query_cells_centroids_list_by_tile, gcm_names, gcm_dates_df))
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
    glm_ready_gcm_data_list$cell_info,
    pattern = map(glm_ready_gcm_data_list),
  ),

  # Pivot the cell_info tibble wider so that we have a single row per cell
  # and can track for how many gcms each cell is or is not missing data
  tar_target(
    glm_ready_gcm_data_cell_status,
    glm_ready_gcm_data_cell_info %>%
      pivot_wider(names_from='gcm', values_from='missing_data', names_glue="{gcm}_missing_data") %>%
      mutate(n_gcm_missing_data = rowSums(across(c(-cell_no,-tile_no)))),
  ),

  # Re-do mapping of which lakes are in which cells and tiles, using only the query
  # cells that returned data and did not have missing data for any variables for any GCMs.
  # Here, each lake is matched to the cell with the centroid that is closest to the
  # lake centroid. By using only those cells that returned data, we can ensure that lakes
  # that fell within cells that were missing data are re-assigned to the closest
  # cell that returned data. This xwalk is being used in `lake-temperature-process-models`
  # to determine what meteorological data to pull for each lake.
  tar_target(lake_cell_tile_xwalk_data_df,
             get_lake_cell_tile_data_xwalk(query_lake_centroids_sf, query_cell_centroids_sf, glm_ready_gcm_data_cell_status)
             ),

  # Save the revised lake-cell-tile mapping for use in `lake-temperature-process-models`
  tar_target(lake_cell_tile_xwalk_csv, {
    out_file <- "7_drivers_munge/out/lake_cell_tile_xwalk.csv"
    write_csv(lake_cell_tile_xwalk_data_df, out_file)
    return(out_file)
  }, format = 'file'),

  # Map the new lake-cell-tile xwalk
  tar_target(
    tile_cell_map_png,
    map_tiles_cells(
      out_file = '7_drivers_munge/out/tile_cell_map.png',
      lake_cell_tile_xwalk = lake_cell_tile_xwalk_data_df,
      grid_tiles = grid_tiles_sf,
      grid_cells = grid_cells_sf
    ),
    format='file'
  ),

  # Create feather files that can be used in the GLM pipeline without
  # having to be munged and extracted via NetCDF. Temporary solution
  # while we work out some NetCDF challenges.
  # TODO: delete once we finish the NetCDF DSG build (see issue #252)
  tar_target(
    out_skipnc_feather, {
      out_dir <- "7_drivers_munge/out_skipnc"
      gcm_name <- str_extract(glm_ready_gcm_data_feather, paste(gcm_names, collapse="|"))
      out_files <- read_feather(glm_ready_gcm_data_feather) %>%
        # Split into data per cell and per time period
        mutate(projectperiod = case_when(
          time <= as.Date("2010-01-01") ~ "1980_1999",
          time <= as.Date("2070-01-01") ~ "2040_2059",
          TRUE ~ "2080_2099",
        )) %>%
        unite("cell_no.projperiod", c("cell_no", "projectperiod"), sep = ".") %>%
        split(.$cell_no.projperiod) %>%
        purrr::map(., function(data) {
          data <- separate(data, cell_no.projperiod, into = c("cell_no", "projection_period"), sep = "\\.")
          cell_no <- unique(data$cell_no)
          projection_period <- unique(data$projection_period)
          data_to_save <- data %>% select(-cell_no, -projection_period)
          out_file <- sprintf("%s/GCM_%s_%s_%s.feather", out_dir, gcm_name, projection_period, cell_no)
          write_feather(data_to_save, out_file)
          return(out_file)
        }) %>% unlist()
      return(out_files)
    },
    pattern = map(glm_ready_gcm_data_feather),
    format = "file"
  ),

  # Group daily feather files by GCM to map over and include
  # branch file hashes to trigger rebuilds for groups as needed
  tar_target(gcm_data_daily_feather_group_by_gcm,
              build_branch_file_hash_table(names(glm_ready_gcm_data_feather)) %>%
               rename(gcm_file = path) %>%
               mutate(gcm_name = str_extract(gcm_file, paste(gcm_names, collapse="|"))) %>%
               group_by(gcm_name) %>%
               tar_group(),
             iteration = "group"),

  # Setup table of GLM variable definitions to use in NetCDF file metadata
  tar_target(glm_vars_info,
             tibble(
               # TODO: MISSING LONGWAVE
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
        global_att = sprintf("GCM Notaro %s", gcm_name),
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
    c(gcm_nc, lake_cell_tile_xwalk_csv, tile_cell_map_png),
    format = 'file'
  )
)

# Return the complete list of targets
c(targets_list)
