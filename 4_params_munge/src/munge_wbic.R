
#' format for these outputs: site_id, {source}_ID, z_max
munge_wbic_depths <- function(out_ind, wbic_wbodies_ind, wbic_xwalk_ind){


  wbic_xwalk <- sc_retrieve(wbic_xwalk_ind) %>% readRDS()

  wbic_depths <- sc_retrieve(wbic_wbodies_ind) %>% read_tsv(col_types = 'ccccdcccdddddc') %>%
    mutate(WBIC_ID = sprintf("WBIC_%s", WBIC), depth = str_remove(OFFICIAL_MAX_DEPTH, " FEET"),
           z_max = convert_ft_to_m(as.numeric(depth))) %>% filter(!is.na(z_max)) %>%
    inner_join(wbic_xwalk, by = 'WBIC_ID') %>% dplyr::select(site_id, WBIC_ID, z_max)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(wbic_depths, data_file)
  gd_put(out_ind, data_file)
}

