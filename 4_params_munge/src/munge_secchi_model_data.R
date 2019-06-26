munge_daily_secchi <- function(in_ind, out_ind, dow_crosswalk_ind) {

  dowfile <- sc_retrieve(dow_crosswalk_ind, remake_file = '1_crosswalk_fetch.yml')
  dow2nhd <- get(load(dowfile))
  dow2nhd$DOW <- as.character(dow2nhd$dowlknum)
  dow2nhd <- distinct(dow2nhd)

  in_dat <- read.csv(sc_retrieve(in_ind))

  out_dat <- in_dat %>%
    mutate(secchi_m = Secchi_ft*0.3048,
           DOW = as.character(DOW)) %>%
    left_join(dplyr::select(dow2nhd, site_id, DOW) , by = 'DOW') %>%
    dplyr::select(-Secchi_ft, -DOW)

  outfile <- as_data_file(out_ind)
  feather::write_feather(out_dat, outfile)
  gd_put(out_ind, outfile)
}
