
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

  # we now use sf > v1.0, which causes issues with some NHD polygons and the spherical coordinates
  # see info on the change: https://r-spatial.org/r/2020/06/17/s2.html#sf-10-goodbye-flat-earth-welcome-s2-spherical-geometry

  # to avoid these NHD issues, we'd either need to edit the files or avoid the error by avoiding use of the
  # S2 engine. We do that by toggling off use of S2 here.
  sf::sf_use_s2(FALSE)
  on.exit(sf::sf_use_s2(TRUE))

  lakes <- scipiper::sc_retrieve(lake_poly_ind) %>% readRDS()
  # multiple matches to

  # multiple lakes in this dataset share the same MGLP ID, (see issue #143)
  # need to "merge" the areas of these multiple
  areas <- data.frame(MGLP_ID = lakes$site_id, areas_m2 = as.numeric(st_area(lakes)), stringsAsFactors = FALSE)

  mglp_xwalk <- readRDS(sc_retrieve(mglp_xwalk_ind)) %>%
    left_join(areas, by = 'MGLP_ID')

  # lakes like MIstjom36571 have >1 name, eg  Havens Lake & Goodrich Lake
  # Two Hearted Lak has multiple long_dd and lat_DD

  # LakeAfr_Be is the fraction surface area at each depth interval (0-1)
  # LakeAfr_Be is multiplied by the surface area in m2 (areas_m2) to get the area at each depth
  bathy_areas <- scipiper::sc_retrieve(bathy_csv_ind) %>% read_csv(col_types = 'ddddccdd') %>%
    mutate(MGLP_ID = paste0('MGLP_', MGLP_ID), depths = Depth_ft * 0.3048) %>%
    inner_join(mglp_xwalk, by = 'MGLP_ID') %>% mutate(areas = LakeAfr_Be * areas_m2) %>%
    # this is the true "unique" ID for the contour, since some MGLP lakes have multiple lakes
    mutate(other_ID = paste0(MGLP_ID, LAKE_NAME, LONG_DD, LAT_DD)) %>%
    dplyr::select(site_id, other_ID, depths, areas) %>%
    collapse_multi_bathy()

  warning('collapsing some odd MI lakes into one')
  data_file <- as_data_file(out_ind)
  saveRDS(bathy_areas, data_file)
  gd_put(out_ind, data_file)

}
