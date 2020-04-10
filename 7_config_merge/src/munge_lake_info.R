

munge_lake_area <- function(out_ind, lakes_ind){

  lakes <- scipiper::sc_retrieve(lakes_ind) %>% readRDS()
  areas <- lakes %>% # should probably do this as a mutate to keep it w/ site_id??
    st_area()

  lake_areas <- data.frame(site_id = lakes$site_id, areas_m2 = as.numeric(areas)) %>%
    group_by(site_id) %>% summarize(areas_m2 = sum(areas_m2)) # Units: [m^2]

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lake_areas, data_file)
  gd_put(out_ind, data_file)
}

munge_cd_from_area <- function(out_ind, areas_ind){

  min_wstr <- 0.0001 # this is the minimum wind sheltering value we'll use
  coef_wind_drag.ref <- 0.00140 # reference cd value which will be scaled
  data_file <- scipiper::as_data_file(out_ind)

  cds <- scipiper::sc_retrieve(areas_ind) %>% readRDS() %>%
    mutate(wstr = 1.0 - exp(-0.3*(areas_m2*1.0e-6))) %>%
    mutate(cd = case_when(
      wstr < min_wstr ~ coef_wind_drag.ref*min_wstr^0.33,
      TRUE ~ coef_wind_drag.ref*wstr^0.33)

    ) %>% dplyr::select(site_id, cd) %>%
    saveRDS(data_file)

  gd_put(out_ind, data_file)
}

munge_Kw <- function(out_ind, secchi_ind, kw_varying_ind, wqp_xwalk_ind){

  data_file <- scipiper::as_data_file(out_ind)

  wqp_xwalk <- scipiper::sc_retrieve(wqp_xwalk_ind) %>% readRDS()
  Kw_data <- scipiper::sc_retrieve(secchi_ind) %>% feather::read_feather() %>%
    inner_join(wqp_xwalk, by = "MonitoringLocationIdentifier") %>% group_by(site_id) %>%
    summarise(Kw = 1.7/mean(secchi, na.rm = TRUE)) %>% filter(!is.na(Kw)) %>%
    dplyr::select(site_id, Kw)

  # only add the mean of the time-varying Kw value when there is no value already
  # save file to .rds
  scipiper::sc_retrieve(kw_varying_ind) %>% readRDS() %>% group_by(site_id) %>%
    summarize(Kw = mean(Kd)) %>% filter(!(site_id %in% Kw_data$site_id)) %>% rbind(Kw_data, .) %>%
    saveRDS(data_file)

  # post and promise the file is posted
  gd_put(out_ind, data_file)
}

#' combines hypsographic data w/ max depth data.
#' @param out_ind out indicator file
#' @param areas_ind indicator file for data.frame that includes site_id and areas_m2
#' @param ... hypso or max depth data.frame indicator files, ranked in priority order
#'
#' @details order matters for `...` If there duplicates for `site_id`, the later files
#' won't replace the values in the earlier ones
#'
#' For lakes that are parameterized with data for max depth,
#' H and A will be two values, a zero area max depth, and a surface area zero depth
munge_H_A <- function(out_ind, areas_ind, ...){
  lake_areas <- scipiper::sc_retrieve(areas_ind) %>% readRDS()
  crest_height <- 320 # magic number for now, elevation of lakes, assumed average for now...

  H_A <- vector("list", length = nrow(lake_areas)) %>%
    setNames(lake_areas$site_id)

  ind_files <- c(...)


  for (ind_file in ind_files){
    still_empty_ids <- names(H_A)[which(sapply(H_A, is.null))]
    depth_data <- scipiper::sc_retrieve(ind_file) %>% readRDS()
    use_ids <- filter(depth_data, site_id %in% still_empty_ids) %>% pull(site_id) %>% unique()

    if (basename(ind_file) %>% stringr::str_detect('bathy')){
      H_A[use_ids] <- lapply(X = use_ids, FUN = function(x) {
        filter(depth_data, site_id == x) %>% arrange(desc(depths)) %>%
          mutate(H = crest_height - depths) %>% dplyr::select(H, A = areas) %>% # duplicate values of H
          group_by(H) %>% summarize(A = mean(A)) %>% ungroup()
      })
    } else { # is max depth
      H_A[use_ids] <- lapply(X = use_ids, FUN = function(x) {
        z_max <- filter(depth_data, site_id == x) %>% pull(z_max) %>% head(1)
        surface_area <- filter(lake_areas, site_id == x) %>% pull(areas_m2) %>% head(1)
        data.frame(H = c(crest_height - z_max, crest_height), A = c(0, surface_area))
      })
    }
  }

  H_A[which(sapply(H_A, is.null))] <- NULL

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(H_A, data_file)
  gd_put(out_ind, data_file)
}

munge_wid_len <- function(out_ind, areas_ind){

  bsn_wid_len <- scipiper::sc_retrieve(areas_ind) %>% readRDS() %>%
    mutate(bsn_wid = sqrt(areas_m2/pi)*2, bsn_len = sqrt(areas_m2/pi)*2) %>%
    dplyr::select(-areas_m2)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(bsn_wid_len, data_file)
  gd_put(out_ind, data_file)

}

munge_lake_depth <- function(out_ind, H_A_ind){
  hypsos <- scipiper::sc_retrieve(H_A_ind) %>% readRDS()

  lake_depth <- data.frame(site_id = names(hypsos), stringsAsFactors = FALSE) %>% rowwise() %>%
    mutate(lake_depth = diff(range(hypsos[[site_id]]$H)))

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lake_depth, data_file)
  gd_put(out_ind, data_file)
}

munge_layer_thick <- function(out_ind, lake_depth_ind){

  # from mda.lakes::populate_base_lake_nml()
  layer_thick <- scipiper::sc_retrieve(lake_depth_ind) %>% readRDS() %>%
    mutate(
      max_layer_thick = case_when(
        lake_depth >= 20 ~ 1.5,
        lake_depth >= 8 & lake_depth < 20 ~ 1,
        lake_depth >= 5 & lake_depth < 8 ~ 0.8,
        lake_depth >= 3 & lake_depth < 5 ~ 0.5,
        TRUE ~ 0.3
      ),
      min_layer_thick = case_when(
        lake_depth >= 3 ~ 0.2,
        TRUE ~ 0.1
      )
    ) %>%
    dplyr::select(-lake_depth)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(layer_thick, data_file)
  gd_put(out_ind, data_file)
}

munge_lat_lon <- function(out_ind, centroids_ind){
  centroids_sf <- scipiper::sc_retrieve(centroids_ind) %>% readRDS()

  # dangerous as it assumes order is retained as we de-couple `site_id`
  lat_lon <- st_coordinates(centroids_sf) %>% as_tibble() %>%
    rename(longitude = X, latitude = Y) %>% mutate(site_id = centroids_sf$site_id) %>%
    dplyr::select(site_id, everything())

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lat_lon, data_file)
  gd_put(out_ind, data_file)
}

munge_meteo_fl <- function(out_ind, centroids_ind, lake_depth_ind, sf_grid, time_range){

  lake_depth <- scipiper::sc_retrieve(lake_depth_ind) %>% readRDS()
  centroids_sf <- scipiper::sc_retrieve(centroids_ind) %>% readRDS() %>%
    filter(site_id %in% lake_depth$site_id) # get rid of lakes we aren't going to model...


  t0 <- time_range[1]
  t1 <- time_range[2]

  meteo_fl <- do(centroids_sf, data.frame(match_collection = st_intersects(sf_grid, st_geometry(.), sparse = FALSE))) %>%
    mutate_all(which) %>% summarise_all(unique) %>%
    tidyr::gather(key = cent_idx, value = grid_idx) %>%
    mutate(x = sf_grid[grid_idx,]$x, y = sf_grid[grid_idx,]$y) %>%
    mutate(site_id = centroids_sf$site_id) %>%
    mutate(meteo_fl = create_meteo_filepath(t0, t1, x, y, dirname = 'drivers') %>% basename()) %>% #using basename to get rid of dir
    dplyr::select(site_id, meteo_fl)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(meteo_fl, data_file)
  gd_put(out_ind, data_file)
}

build_nml_list <- function(
  H_A_ind = '7_config_merge/out/nml_H_A_values.rds.ind',
  cd_ind = '7_config_merge/out/nml_cd_values.rds.ind',
  lat_lon_ind = '7_config_merge/out/nml_lat_lon_values.rds.ind',
  len_wid_ind = '7_config_merge/out/nml_len_wid_values.rds.ind',
  lake_depth_ind = '7_config_merge/out/nml_lake_depth_values.rds.ind',
  layer_thick_ind = '7_config_merge/out/nml_layer_thick_values.rds.ind',
  meteo_fl_ind = '7_config_merge/out/nml_meteo_fl_values.rds.ind',
  kw_ind = '7_config_merge/out/nml_Kw_values.rds.ind',
  out_ind){

  # combine the model parameters into a single data.frame
  nml_df_data <- readRDS(scipiper::sc_retrieve(cd_ind)) %>%
    inner_join(readRDS(scipiper::sc_retrieve(lat_lon_ind)), by = 'site_id') %>%
    inner_join(readRDS(scipiper::sc_retrieve(len_wid_ind)), by = 'site_id') %>%
    inner_join(readRDS(scipiper::sc_retrieve(lake_depth_ind)), by = 'site_id') %>%
    inner_join(readRDS(scipiper::sc_retrieve(layer_thick_ind)), by = 'site_id') %>%
    inner_join(readRDS(scipiper::sc_retrieve(meteo_fl_ind)), by = 'site_id') %>%
    inner_join(readRDS(scipiper::sc_retrieve(kw_ind)), by = 'site_id')
    # ideally would only include those sites for which the meteo file was
    # actually created, but in practice this was a huge headache on 1/15/2020,
    # so for now we'll do that filtering in lake-temperature-process-modeling.
    # if we had a reliable list of files in 7_drivers_munge_tasks.ind we'd do:
    #    filter(meteo_fl %in% basename(names(yaml::read_yaml(meteo_fl_list))))

  # convert df to list where each row of the former df becomes a list element
  nml_list <- split(nml_df_data, seq(nrow(nml_df_data))) %>% setNames(nml_df_data$site_id)

  # merge height & area info with the nlm_list. note the file referred to by H_A_ind is a list already
  H_A_list <- readRDS(scipiper::sc_retrieve(H_A_ind))
  H_A_site_ids <- names(H_A_list)
  for (id in H_A_site_ids[H_A_site_ids %in% names(nml_list)]){
    nml_list[[id]] = as.list(nml_list[[id]])
    nml_list[[id]]$A = H_A_list[[id]]$A
    nml_list[[id]]$H = H_A_list[[id]]$H
  }

  saveRDS(nml_list, scipiper::as_data_file(out_ind))
  gd_put(out_ind)
}

