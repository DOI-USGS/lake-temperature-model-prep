

mglp_depths_from_TDOx <- function(out_ind, TDOx_ind){

  TDOx_data <- sc_retrieve(TDOx_ind) %>% read_csv

  mglp_depths <- TDOx_data %>% mutate(site_id = sprintf("mglp_%s", mglp_id)) %>%
    dplyr::select(site_id, z_max = max_depth)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(mglp_depths, data_file)
  gd_put(out_ind, data_file)
}
