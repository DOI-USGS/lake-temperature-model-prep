
merge_lake_data <- function(out_ind, temp_data_fl, lake_names_ind, lake_loc_ind, lake_data_ind,
                            lagos_xwalk_ind, MGLP_xwalk_ind, WBIC_xwalk_ind, Micorps_xwalk_ind,
                            MNDOW_xwalk_ind, Winslow_xwalk_ind, NDGF_xwalk_ind, kw_ind,
                            meteo_ind, kw_file_fl ){

  temp_dat <- feather::read_feather(temp_data_fl)
  lake_names <- readRDS(sc_retrieve(lake_names_ind))
  lake_loc <- readRDS(sc_retrieve(lake_loc_ind))
  lake_data <- readRDS(sc_retrieve(lake_data_ind))


  kw_file_ids <- readRDS(kw_file_fl)
  kw_val_ids <- readRDS(sc_retrieve(kw_ind))[["site_id"]]
  meteo_file_ids <- readRDS(sc_retrieve(meteo_ind)) %>%
    filter(file.exists(file.path('7_drivers_munge/out/',meteo_fl))) %>% pull(site_id)

  # Read xwalks
  lagos_xwalk <- readRDS(sc_retrieve(lagos_xwalk_ind))[["site_id"]]
  MGLP_xwalk <- readRDS(sc_retrieve(MGLP_xwalk_ind))[["site_id"]]
  WBIC_xwalk <- readRDS(sc_retrieve(WBIC_xwalk_ind))[["site_id"]]
  Micorps_xwalk <- readRDS(sc_retrieve(Micorps_xwalk_ind))[["site_id"]]
  MNDOW_xwalk <- readRDS(sc_retrieve(MNDOW_xwalk_ind))[["site_id"]]
  Winslow_xwalk <- readRDS(sc_retrieve(Winslow_xwalk_ind))[["site_id"]]
  NDGF_xwalk <- readRDS(sc_retrieve(NDGF_xwalk_ind))[["site_id"]]

  # Format data

  total_obs <- temp_dat %>%
    group_by(site_id) %>%
    summarize(n_obs = n())

  lake_days <- temp_dat %>%
    group_by(site_id, date) %>%
    summarize(n_depths = n()) %>%
    filter(n_depths >= 5) %>%
    ungroup() %>%
    mutate(year = lubridate::year(date)) %>%
    group_by(site_id) %>%
    summarize(n_profiles = n())

  profile_years <- temp_dat %>%
    group_by(site_id, date) %>%
    summarize(n_depths = n()) %>%
    filter(n_depths >= 5) %>%
    ungroup() %>%
    mutate(year = lubridate::year(date)) %>%
    group_by(site_id, year) %>%
    summarize(n_profiles = n()) %>%
    filter(n_profiles >5) %>%
    group_by(site_id) %>%
    summarize(n_years_6profs = n())

  # Figure out which lakes have zmax and which have hypso
  all_lakes <- unique(lake_names$site_id)
  has_zmax <- all_lakes %in% names(lake_data) # any lake in this dataset has zmax
  has_kw <- all_lakes %in% kw_val_ids
  has_kw_file <- all_lakes %in% kw_file_ids
  has_meteo <- all_lakes %in% meteo_file_ids
  has_hypso <- lapply(all_lakes, function(id) {
    # First check that lake has data
    if(id %in% names(lake_data)) {
      # When lakes don't have hypso, default is to put two depths: surface (H = 320, A = real #) & zmax (H = real #, A = 0)
      # It is unlikely that a lake with hypso data only has it for 2 depths (likely at least 3), so any lake that has more
      # than those 2, we will say has hypso data.
      depths_id <- lake_data[[id]]$H
      has_hypso <- length(depths_id) > 2
    } else {
      # If lake is not in the lake data, then it doesn't have hypso
      has_hypso <- FALSE
    }
    return(has_hypso)
  }) %>% unlist()

  metadata <- data.frame(site_id = all_lakes,
                         zmax = has_zmax,
                         hypsography = has_hypso,
                         kw = has_kw,
                         kw_file = has_kw_file,
                         meteo = has_meteo,
                         stringsAsFactors = FALSE)

  # Combine everything into one dataset
  lake_summary <- lake_names %>%
    left_join(total_obs) %>%
    left_join(lake_days) %>%
    left_join(profile_years) %>%
    left_join(rename(lake_loc)) %>%
    left_join(metadata)

  all_real <- function(x) !all(is.na(x))
  lake_summary_w_xwalk <- lake_summary %>%
    mutate(Source_LAGOS = ifelse(site_id %in% lagos_xwalk, "Lagos", NA),
           Source_MGLP = ifelse(site_id %in% MGLP_xwalk, "MGLP", NA),
           Source_WBIC = ifelse(site_id %in% WBIC_xwalk, "WBIC", NA),
           Source_Micorps = ifelse(site_id %in% Micorps_xwalk, "Micorps", NA),
           Source_MNDOW = ifelse(site_id %in% MNDOW_xwalk, "MNDOW", NA),
           Source_Winslow = ifelse(site_id %in% Winslow_xwalk, "Winslow", NA),
           Source_NDGF = ifelse(site_id %in% NDGF_xwalk, "NDGF", NA)) %>%
    # unite doesn't work as expected if a column is all NA (https://github.com/tidyverse/tidyr/issues/203#issuecomment-590931838)
    # currently, micorps is not all real
    select_if(all_real) %>%
    unite("Lake_Source", starts_with("Source_"), sep = " | ", na.rm = TRUE)

  outfile <- as_data_file(out_ind)
  feather::write_feather(lake_summary_w_xwalk, outfile)
  gd_put(out_ind)

}

# Not pipeline ready yet, but so that you can see what I did
summary_MN_lake_data <- function() {

  mndow_xwalk <- readRDS(sc_retrieve("2_crosswalk_munge/out/mndow_nhdhr_xwalk.rds.ind"))

  # this data downloaded here: https://drive.google.com/drive/u/4/folders/13w3QDXicjHVymVW1NeyJpJqVRZ7KvlTD
  walleye_df <- read_csv("WAE_numYears_meanCPUE.csv") %>%
    mutate(MNDOW_ID = sprintf("mndow_%s", DOW)) %>%
    rename(n_walleye_yrs = n_yrs) %>%
    left_join(mndow_xwalk) %>%
    dplyr::select(site_id, MNDOW_ID, n_walleye_yrs)

  # this data downloaded here: https://drive.google.com/drive/u/4/folders/13w3QDXicjHVymVW1NeyJpJqVRZ7KvlTD
  time_varying_kw_sites <- readRDS("nhdhr_ids_time_varying_clarity.rds")

  lake_summary_data <- read_feather("8_viz/inout/lakes_summary.feather") %>%
    rename(n_temp_obs = n_obs) %>%
    left_join(walleye_df) %>%
    mutate(has_time_varying_kw = site_id %in% time_varying_kw_sites)

  all_criteria <- lake_summary_data %>%
    filter(n_profiles >= 10,
           n_walleye_yrs >= 5, # already doing this in CSV
           has_time_varying_kw)

  # Wouldn't just be MN ...
  # n_lakes_with_profiles <- lake_summary_data %>%
  #   filter(n_profiles >= 10) %>%
  #   nrow()
  #
  # n_lakes_with_timevaryingkw <- lake_summary_data %>%
  #   filter(has_time_varying_kw) %>%
  #   nrow()
  #
  # n_lakes_with_walleye <- nrow(walleye_df)

  return(all_criteria)
}
