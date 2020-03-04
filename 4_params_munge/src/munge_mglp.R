
#' format for these outputs: site_id, {source}_ID, z_max
mglp_depths_from_TDOx <- function(out_ind, TDOx_ind, mglp_xwalk_ind){


  mglp_xwalk <- sc_retrieve(mglp_xwalk_ind) %>% readRDS()
  mglp_depths <- sc_retrieve(TDOx_ind) %>% read_csv() %>% mutate(MGLP_ID = sprintf("MGLP_%s", mglp_id)) %>%
    inner_join(mglp_xwalk, by = 'MGLP_ID') %>% dplyr::select(site_id, MGLP_ID, z_max = max_depth)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(mglp_depths, data_file)
  gd_put(out_ind, data_file)
}


munge_mglp_mi_perc_bathy <- function(out_ind, bathy_csv_ind, mglp_xwalk_ind, lake_poly_ind){


  lakes <- scipiper::sc_retrieve(lake_poly_ind) %>% readRDS()
  # multiple matches to

  areas <- data.frame(MGLP_ID = lakes$site_id, areas = as.numeric(st_area(lakes)), stringsAsFactors = FALSE) %>%
    group_by(MGLP_ID) %>% summarize(areas_m2 = sum(areas))

  mglp_xwalk <- readRDS(sc_retrieve(mglp_xwalk_ind)) %>%
    left_join(areas, by = 'MGLP_ID')

  bathy_areas <- scipiper::sc_retrieve(bathy_csv_ind) %>% read_csv %>%
    mutate(MGLP_ID = paste0('MGLP_', MGLP_ID), depths = Depth_ft * 0.3048) %>%
    left_join(mglp_xwalk) %>%



  browser()
  bth_dir <- tempdir()

  # this still has the state-specific ID, since some NHDHR ids (site_id) have more than one. We need to combine those later
  bathy_data_mapped <- sc_retrieve(bathy_zip_ind) %>% unzip(exdir = bth_dir) %>%
    purrr::map(function(x) {
      MNDOW_ID <- basename(x) %>% str_extract('[0-9]+.csv$') %>% str_extract('[0-9]+') %>% paste0('mndow_', .)
      data <- {read_csv(x)[,1:2]} %>% setNames(c('depth_feet', 'proportion_area')) %>%
        mutate(MNDOW_ID = MNDOW_ID, depths = depth_feet * 0.3048) %>%
        dplyr::select(-depth_feet)
    }) %>% purrr::reduce(rbind) %>%
    inner_join(mndow_xwalk, by = 'MNDOW_ID') %>%
    mutate(areas = proportion_area * areas_m2) %>% rename(other_ID = MNDOW_ID) %>%
    dplyr::select(site_id, other_ID, depths, areas)

  data_file <- scipiper::as_data_file(out_ind)
  collapse_multi_bathy(bathy_data_mapped) %>%
    saveRDS(data_file)
  gd_put(out_ind, data_file)

}
