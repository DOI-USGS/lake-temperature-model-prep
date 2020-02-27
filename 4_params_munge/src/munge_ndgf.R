
#' format for these outputs: site_id, {source}_ID, z_max
munge_ndgf_depths <- function(out_ind, ndgf_depths_ind, ndgf_xwalk_ind){

  ndgf_xwalk <- sc_retrieve(ndgf_xwalk_ind) %>% readRDS()

  ndgf_depths <- sc_retrieve(ndgf_depths_ind) %>% readRDS() %>%
    mutate(NDGF_ID = sprintf("ndgf_%s", LAKE), depth = DEPTH,
           z_max = as.numeric(depth)) %>% filter(!is.na(z_max)) %>%
    inner_join(ndgf_xwalk, by = 'NDGF_ID') %>% dplyr::select(site_id, NDGF_ID, z_max)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(ndgf_depths, data_file)
  gd_put(out_ind, data_file)
}
