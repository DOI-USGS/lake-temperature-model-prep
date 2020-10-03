count_lake_obs <- function(
  temp_obs_ind = '7b_temp_merge/out/temp_data_with_sources.feather.ind',
  nml_list_ind = '7_config_merge/out/nml_list.rds.ind',
  obs_thresholds = c(0, 1, 5, 10, 20, 30)) {

  # Read the data
  temp_data <- feather::read_feather(scipiper::as_data_file(temp_obs_ind)) # replace as_data_file with sc_retrieve when practical
  nml_list <- readRDS(scipiper::as_data_file(nml_list_ind)) # replace as_data_file with sc_retrieve when practical

  # Generate table of relevant counts
  obs_counts <- temp_data %>%
    group_by(site_id, date) %>%
    summarize(n_per_date = n(), .groups='drop_last') %>%
    full_join(tibble(site_id = names(nml_list), glm_ready = TRUE), by='site_id') %>%
    mutate(
      n_per_date = replace_na(n_per_date, 0),
      glm_ready = replace_na(glm_ready, FALSE))

  # Count numbers of sites for each combo of min_obs_per_date and min_obs_dates
  count_sites <- function(obs_counts, obs_thresholds) {
    max_obs_per_date <- max(obs_counts$n_per_date)
    max_obs_dates <- max(summarize(obs_counts, n_obs_dates = length(date), .groups='drop')$n_obs_dates)
    mins_obs_per_date <- c(obs_thresholds, max_obs_per_date)
    mins_obs_dates <- 0:max_obs_dates
    site_counts_mat <- matrix(
      NA, nrow=length(mins_obs_per_date), ncol=length(mins_obs_dates),
      dimnames=list(min_obs_per_date=mins_obs_per_date, min_obs_dates=mins_obs_dates))
    if(0 %in% obs_thresholds) {
      site_counts_mat['0', '0'] <- obs_counts %>% pull(site_id) %>% unique() %>% length()
    }
    for(min_obs_per_date in mins_obs_per_date[-which(mins_obs_per_date == 0)]) {
      message('!', appendLF = FALSE)
      date_counts <- obs_counts %>%
        filter(n_per_date >= min_obs_per_date, !is.na(date)) %>%
        group_by(site_id) %>%
        summarize(n_obs_dates = n(), .groups='drop')
      for(min_obs_dates in mins_obs_dates[-which(mins_obs_dates == 0)]) {
        if(min_obs_dates %% 1000 == 0) message('.', appendLF=FALSE)
        date_counts <- filter(date_counts, n_obs_dates >= min_obs_dates)
        site_counts_mat[as.character(min_obs_per_date), as.character(min_obs_dates)] <- nrow(date_counts)
      }
    }
    message(appendLF = TRUE)
    site_counts <- as.data.frame(site_counts_mat) %>%
      tibble::rownames_to_column('obs_per_date') %>%
      as_tibble() %>%
      tidyr::gather(obs_dates, n_sites, -obs_per_date) %>%
      mutate(obs_dates = as.numeric(obs_dates), obs_per_date = as.numeric(obs_per_date)) %>%
      filter(!(obs_per_date == 0 & obs_dates > 0)) %>%
      arrange(desc(obs_per_date), desc(obs_dates))

    return(site_counts)
  }
  site_counts <-
    bind_rows(
      count_sites(obs_counts, obs_thresholds) %>% mutate(has_glm = 'Yes or No'),
      count_sites(dplyr::filter(obs_counts, glm_ready), obs_thresholds) %>% mutate(has_glm = 'Yes only')) %>%
    filter(obs_per_date %in% obs_thresholds) %>%
    mutate(obs_per_date = ordered(obs_per_date))

  return(site_counts)
}
