
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

  # lakes like MIstjom36571 have >1 name, eg  Havens Lake & Goodrich Lake
  # Two Hearted Lak has multiple long_dd and lat_DD

  bathy_areas <- scipiper::sc_retrieve(bathy_csv_ind) %>% read_csv %>%
    mutate(MGLP_ID = paste0('MGLP_', MGLP_ID), depths = Depth_ft * 0.3048) %>%
    inner_join(mglp_xwalk) %>% mutate(areas = LakeAfr_Be * areas_m2) %>%
    mutate(other_ID = paste0(MGLP_ID, LAKE_NAME, LONG_DD, LAT_DD)) %>%
    dplyr::select(site_id, other_ID, depths, areas) %>%
    collapse_multi_bathy()

  warning('collapsing some odd MI lakes into one')
  data_file <- as_data_file(out_ind)
  saveRDS(bathy_areas, data_file)
  gd_put(out_ind, data_file)

}
