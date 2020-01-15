#' Plot lake counts by minimum number of depths per date, minimum number of
#' dates, and number of lakes meeting each combination of those criteria
#'
#' Until the nml_list is filtered by availability of meteo_fl, 'Has GLM params =
#' Yes only' is a slight superset of the sites for which we actually have GLM
#' models (difference appears to be 6 models as of 1/15/2020)
plot_lake_obs_counts <- function(
  out_file = '1_format/doc/lake_obs_counts.png',
  temp_obs_ind = '7b_temp_merge/out/temp_data_with_sources.feather.ind',
  nml_list_ind = '7_config_merge/out/nml_list.rds.ind') {

  # Read the data
  temp_data <- feather::read_feather(scipiper::as_data_file(temp_obs_ind)) # swap sc_retrieve for as_data_file once coop data is updated
  nml_list <- readRDS(scipiper::sc_retrieve(nml_list_ind))

  # Generate table of relevant counts
  obs_counts <- temp_data %>%
    group_by(site_id, date) %>%
    summarize(n_per_date = n()) %>%
    full_join(tibble(site_id = names(nml_list), glm_ready = TRUE), by='site_id') %>%
    mutate(
      n_per_date = replace_na(n_per_date, 0),
      glm_ready = replace_na(glm_ready, FALSE))

  # Count numbers of sites for each combo of min_obs_per_date and min_obs_dates
  count_sites <- function(obs_counts) {
    max_obs_per_date <- max(obs_counts$n_per_date)
    max_obs_dates <- max(summarize(obs_counts, n_obs_dates = length(date))$n_obs_dates)
    mins_obs_per_date <- c(0, 1, 5, 10, 20, 30, max_obs_per_date)
    mins_obs_dates <- 0:max_obs_dates
    site_counts_mat <- matrix(
      NA, nrow=length(mins_obs_per_date), ncol=length(mins_obs_dates),
      dimnames=list(min_obs_per_date=mins_obs_per_date, min_obs_dates=mins_obs_dates))
    for(min_obs_per_date in mins_obs_per_date) {
      message('.', appendLF = FALSE)
      date_counts <- obs_counts %>%
        filter(n_per_date >= min_obs_per_date) %>%
        summarize(n_obs_dates = length(which(!is.na(date))))
      for(min_obs_dates in mins_obs_dates) {
        site_counts_mat[as.character(min_obs_per_date), as.character(min_obs_dates)] <-
          if(xor(min_obs_per_date == 0, min_obs_dates == 0)) {
            NA
          } else {
            date_counts <- filter(date_counts, n_obs_dates >= min_obs_dates)
            nrow(date_counts)
          }

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
      count_sites(obs_counts) %>% mutate(has_glm = 'Yes or No'),
      count_sites(dplyr::filter(obs_counts, glm_ready)) %>% mutate(has_glm = 'Yes only')) %>%
    filter(obs_per_date %in% c(0,1,5,10,20,30)) %>%
    mutate(obs_per_date = ordered(obs_per_date))

  ggplot(data=site_counts, aes(x=obs_dates, y=n_sites, color=obs_per_date)) +
    geom_line(data=filter(site_counts, obs_dates > 0), aes(linetype=has_glm)) +
    geom_point(data=filter(site_counts, obs_dates == 0), aes(shape=has_glm)) +
    theme_bw() +
    scale_x_log10('Minimum Observation Dates', breaks=rep(c(1,2,5), times=4)*rep(c(1,10,100,1000), each=3), minor_breaks=NULL) +
    scale_y_log10('Number of Lakes Meeting Criteria', breaks=rep(c(1,2,5), times=4)*rep(c(1,10,100,1000), each=3), minor_breaks=NULL) +
    scale_color_discrete('Min Depths per Date', breaks=c(0,1,5,10,20,30)) +
    scale_linetype_discrete('Has GLM params') +
    scale_shape_discrete('Has GLM params')

  if(!dir.exists(dirname(out_file))) dir.create(dirname(out_file))
  ggsave(out_file, width=7, height=6)
}
