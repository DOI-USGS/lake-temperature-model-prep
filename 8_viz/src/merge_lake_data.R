
merge_lake_data <- function(out_ind, temp_data_fl, lake_depth_ind, lake_names_ind, lake_loc_ind, lake_data_ind,
                            lagos_xwalk_ind, MGLP_xwalk_ind, WBIC_xwalk_ind, Micorps_xwalk_ind,
                            MNDOW_xwalk_ind, Winslow_xwalk_ind, NDGF_xwalk_ind, kw_ind,
                            meteo_ind, meteo_files_ind, toha_varying_kw_ind, digitzing_hypos_ind){

  temp_dat <- feather::read_feather(temp_data_fl)
  lake_names <- readRDS(sc_retrieve(lake_names_ind))
  lake_loc <- readRDS(sc_retrieve(lake_loc_ind))
  lake_data <- readRDS(sc_retrieve(lake_data_ind))
  these_depths <- readRDS(sc_retrieve(lake_depth_ind))


  kw_file_ids <- readRDS(sc_retrieve(toha_varying_kw_ind)) %>% pull(site_id) %>% unique()
  kw_val_ids <- readRDS(sc_retrieve(kw_ind))[["site_id"]]
  local_meteo_files <- readRDS(sc_retrieve(meteo_files_ind))[["local_driver"]]

  meteo_file_ids <- readRDS(sc_retrieve(meteo_ind)) %>%
    filter(meteo_fl %in% local_meteo_files) %>% pull(site_id)
  wi_digitizing_ids <- readRDS(sc_retrieve(digitzing_hypos_ind)) %>% pull(site_id)

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



  # find the min number of obs per date requirement for each lake by including `lake_depth` and `min_depth_density`

  min_depth_density <- 0.5
  min_depths <- 5
  obs_requirements <- mutate(these_depths, min_obs = min(ceiling(lake_depth * min_depth_density), min_depths))

  lake_days <- temp_dat %>%
    inner_join(obs_requirements) %>%
    dplyr::select(-lake_depth) %>%
    group_by(site_id, date) %>%
    summarize(n_depths = n(), min_obs = unique(min_obs)) %>%
    filter(n_depths >= min_obs) %>%
    ungroup() %>%
    mutate(year = lubridate::year(date)) %>%
    group_by(site_id) %>%
    summarize(n_profiles = n())

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
  will_have_hypso <- all_lakes %in% wi_digitizing_ids

  metadata <- data.frame(site_id = all_lakes,
                         zmax = has_zmax,
                         hypsography = has_hypso,
                         hypsography_digitizing = will_have_hypso,
                         kw = has_kw,
                         kw_file = has_kw_file,
                         meteo = has_meteo,
                         stringsAsFactors = FALSE)

  # Combine everything into one dataset
  lake_summary <- lake_names %>%
    left_join(total_obs) %>%
    left_join(lake_days) %>%
    left_join(rename(lake_loc)) %>%
    left_join(metadata) %>%
    mutate(obs_category = dplyr::case_when(
      is.na(n_profiles) ~ 'none',
      n_profiles < 10 ~ '< 10 profiles',
      n_profiles < 50 & n_profiles >= 10 ~ '< 50 profiles',
      n_profiles >= 50 ~ '50+ profiles'
    ))

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

# Summarizing the information related to TOHA models
summarize_MN_toha_lake_data <- function(out_ind, mndow_xwalk_ind, lake_summary_ind,
                                        walleye_count_ind, plant_priority_ind) {

  mndow_xwalk <- readRDS(sc_retrieve(mndow_xwalk_ind))
  walleye_count_data <- read_csv(sc_retrieve(walleye_count_ind))
  plant_priority_data <- read_csv(sc_retrieve(plant_priority_ind))

  walleye_df <- walleye_count_data %>%
    mutate(MNDOW_ID = sprintf("mndow_%s", DOW)) %>%
    rename(n_walleye_yrs = n_yrs) %>%
    left_join(mndow_xwalk) %>%
    dplyr::select(site_id, MNDOW_ID, n_walleye_yrs) %>%
    # Remove sites with the same NHDHR ID but multiple MNDOWs
    #   by taking the max of the walleye yrs
    group_by(site_id) %>%
    summarize(n_walleye_yrs = max(n_walleye_yrs))

  plant_priority_sites <- plant_priority_data[["site_id"]]

  lake_summary_data <- read_feather(sc_retrieve(lake_summary_ind)) %>%
    rename(n_temp_obs = n_obs,
           has_time_varying_kw = kw_file) %>%
    left_join(walleye_df) %>%
    mutate(EWR_Lake = site_id %in% plant_priority_sites)

  outfile <- as_data_file(out_ind)
  saveRDS(lake_summary_data, outfile)
  gd_put(out_ind)

}
