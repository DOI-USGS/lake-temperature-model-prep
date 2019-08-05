
#' format for these outputs: site_id, {source}_ID, z_max
mglp_depths_from_TDOx <- function(out_ind, TDOx_ind, mglp_xwalk_ind){


  mglp_xwalk <- sc_retrieve(mglp_xwalk_ind) %>% readRDS()
  mglp_depths <- sc_retrieve(TDOx_ind) %>% read_csv() %>% mutate(MGLP_ID = sprintf("MGLP_%s", mglp_id)) %>%
    inner_join(mglp_xwalk, by = 'MGLP_ID') %>% dplyr::select(site_id, MGLP_ID, z_max = max_depth)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(mglp_depths, data_file)
  gd_put(out_ind, data_file)
}
