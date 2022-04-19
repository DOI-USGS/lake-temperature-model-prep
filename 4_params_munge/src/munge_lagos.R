
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
      lake_namegnis = col_character(),
      lake_states = col_character(),
      lake_depth_state = col_character(),
      lake_lat_decdeg = col_double(),
      lake_lon_decdeg = col_double(),
      lake_maxdepth_m = col_double(),
      lake_meandepth_m = col_double(),
      lake_waterarea_ha = col_double(),
      lake_depth_sourcename = col_character(),
      lake_depth_sourceurl = col_character(),
      lake_maxdepth_effort = col_character(),
      lake_meandepth_effort = col_character())) %>%
    mutate(LAGOS_ID = sprintf("lagos_%s", lagoslakeid)) %>%
    # Missing sources came in as the string "NULL", changing to NA instead
    mutate(z_max_source = ifelse(lake_depth_sourcename == "NULL", NA, lake_depth_sourcename)) %>%
    dplyr::inner_join(lagos_xwalk, by = 'LAGOS_ID') %>%
    dplyr::select(site_id, LAGOS_ID, z_max_source, z_max = lake_maxdepth_m)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lagos_depths, data_file)
  gd_put(out_ind, data_file)

}
