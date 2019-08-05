
#' format for these outputs: site_id, {source}_ID, mon_group, date, secchi
munge_lagos_secchi <- function(out_ind, lagos_secchi_ind, lagos_xwalk_ind){

  lagos_xwalk <- sc_retrieve(lagos_xwalk_ind) %>% readRDS()
  lagos_secchi <- sc_retrieve(lagos_secchi_ind) %>%
    read_csv(col_types = cols(
      eventidc1087 = col_double(),
      lagoslakeid = col_double(),
      programname = col_character(),
      programtype = col_character(),
      lagosversion = col_character(),
      sampledate = col_date(format = "%m/%d/%Y"),
      secchi = col_double(),
      secchi_censorcode = col_character(),
      secchi_qual = col_character(),
      secchi_methodinfo = col_character(),
      sampleyear = col_double(),
      samplemonth = col_double())) %>%
    mutate(LAGOS_ID = sprintf("lagos_%s", lagoslakeid)) %>%
    dplyr::inner_join(lagos_xwalk, by = 'LAGOS_ID') %>%
    dplyr::select(site_id, LAGOS_ID, mon_group = programname, date = sampledate, secchi)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lagos_secchi, data_file)
  gd_put(out_ind, data_file)

}


#' format for these outputs: site_id, {source}_ID, z_max_source, z_max
munge_lagos_depths <- function(out_ind, lagos_depths_ind, lagos_xwalk_ind){

  lagos_xwalk <- sc_retrieve(lagos_xwalk_ind) %>% readRDS()

  lagos_depths <- sc_retrieve(lagos_depths_ind) %>%
    read_csv(col_types = cols(
      lagoslakeid = col_double(),
      nhdid = col_character(),
      nhd_lat = col_double(),
      nhd_long  = col_double(),
      lagosname1 = col_character(),
      meandepth = col_double(),
      meandepthsource = col_character(),
      zmaxobs = col_double(),
      maxdepthsource = col_character(),
      legacyid = col_character())) %>%
    mutate(LAGOS_ID = sprintf("lagos_%s", lagoslakeid)) %>%
    dplyr::inner_join(lagos_xwalk, by = 'LAGOS_ID') %>%
    dplyr::select(site_id, LAGOS_ID, z_max_source = maxdepthsource, z_max = zmaxobs)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lagos_depths, data_file)
  gd_put(out_ind, data_file)

}
