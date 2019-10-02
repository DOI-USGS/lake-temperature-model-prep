

munge_cd_from_nlcd <- function(out_ind, nlcd_ind, nlcd_hc_file, areas_ind){
  # need hc (canopy height) from nlcd class
  min_wstr <- 0.0001 # this is the minimum wind sheltering value we'll use
  coef_wind_drag.ref <- 0.00140 # reference cd value which will be scaled

  # hc = "canopy height" in meters of various landcover types
  nlcd_hc <- readr::read_tsv(nlcd_hc_file, col_types = 'idc') %>%
    dplyr::select(nlcd_class = nlcd.class, hc_value = hc.value) %>%
    rbind(data.frame(nlcd_class = 0, hc_value = 0)) # great lakes seem to have "0"?


  nlcd_frac_classes <- scipiper::sc_retrieve(nlcd_ind) %>% readRDS()
  lake_areas <- scipiper::sc_retrieve(areas_ind) %>% readRDS()

  blank_nlcd_frac_classes <- nlcd_frac_classes %>% dplyr::select(-id)

  # modified from mda.lakes::getCanopy(), mda.lakes::getWstr(), and mda.lakes::getCD()
  lake_hc <- nlcd_frac_classes %>% # this following line gets the name of the column for the max canopy fraction in each row
    mutate(max_class_name = colnames(blank_nlcd_frac_classes )[max.col(blank_nlcd_frac_classes ,ties.method="first")]) %>%
    rowwise() %>%
    mutate(nlcd_class = as.integer(strsplit(max_class_name,'[.]')[[1]][2])) %>%
    dplyr::select(site_id = id, nlcd_class) %>%
    left_join(nlcd_hc) %>%
    left_join(lake_areas) %>%
    mutate(Xt = 50 * hc_value, # equations from Markford et al. 2010
           D = 2*sqrt(areas_m2/pi),
           shelter = case_when(D < Xt ~ min_wstr,
                               TRUE ~ 2/pi*acos(Xt/D)-(2*Xt/(pi*D^2))*sqrt(D^2-Xt^2)),
           cd = coef_wind_drag.ref*shelter^0.33) %>%
    dplyr::select(site_id, cd)

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(lake_hc, data_file)
  gd_put(out_ind, data_file)
}
