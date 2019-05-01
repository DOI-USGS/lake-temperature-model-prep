

munge_lagos_secchi <- function(out_ind, lagos_secchi_ind){

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
    mutate(site_id = sprintf("lagos_%s", lagoslakeid)) %>%
    dplyr::select(site_id, date = sampledate, secchi, mon_group = programname)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lagos_secchi, data_file)
  gd_put(out_ind, data_file)

}
