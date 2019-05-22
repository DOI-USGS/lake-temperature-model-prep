
merge_lake_data <- function(out_ind, temp_data_ind, lake_names_ind){
  temp_dat <- feather::read_feather(sc_retrieve(temp_data_ind))
  lakenames <- readRDS(sc_retrieve(lake_names_ind))

  total_obs <- temp_dat %>%
    group_by(nhd_id) %>%
    summarize(n_obs = n())

  lake_days <- temp_dat %>%
    group_by(nhd_id, date) %>%
    summarize(n_depths = n()) %>%
    filter(n_depths >= 5) %>%
    ungroup() %>%
    mutate(year = lubridate::year(date)) %>%
    group_by(nhd_id) %>%
    summarize(n_profiles = n(),
              n_profile_years = length(unique(year)))

  zmax_true <- unique(lakeattributes::zmax$site_id)

  all_lakes <- unique(lakenames$site_id)
  hypso_true <- function(lake_id) {
    temp_hypso <- lakeattributes::get_bathy(site_id = lake_id, cone_est = FALSE)
    return(ifelse(is.null(temp_hypso), FALSE, TRUE))
  }

  hypso_dat <- data.frame(site_id = all_lakes,
                          hypsometry = sapply(all_lakes, hypso_true))


  lake_summary <- dplyr::select(lakenames, site_id, lake_name) %>%
    left_join(rename(total_obs, site_id = nhd_id)) %>%
    left_join(rename(lake_days, site_id = nhd_id)) %>%
    left_join(lakeattributes::location) %>%
    mutate(zmax = ifelse(site_id %in% zmax_true, TRUE, FALSE)) %>%
    left_join(hypso_dat)


  outfile <- as_data_file(out_ind)
  feather::write_feather(lake_summary, outfile)
  gd_put(out_ind)

}
