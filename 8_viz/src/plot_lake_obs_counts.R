#' Plot lake counts by minimum number of depths per date, minimum number of
#' dates, and number of lakes meeting each combination of those criteria
#'
#' Until the nml_list is filtered by availability of meteo_fl, 'Has GLM params =
#' Yes only' is a slight superset of the sites for which we actually have GLM
#' models (difference appears to be 6 models as of 1/15/2020)
plot_lake_obs_counts <- function(
  out_file = '1_format/doc/lake_obs_counts.png',
  temp_obs_ind = '7b_temp_merge/out/temp_data_with_sources.feather.ind',
  nml_list_ind = '7_config_merge/out/nml_list.rds.ind',
  obs_thresholds = c(0,1) #c(0,1,5,10,20,30)
) {

  site_counts <- count_lake_obs(temp_obs_ind, nml_list_ind, obs_thresholds=obs_thresholds)#, 5, 10, 20, 30))

  ggplot(data=site_counts, aes(x=obs_dates, y=n_sites, color=obs_per_date)) +
    geom_line(data=filter(site_counts, obs_dates > 0), aes(linetype=has_glm)) +
    geom_point(data=filter(site_counts, obs_dates == 0) %>% mutate(n_sites = ifelse(has_glm == 'Yes only', n_sites, NA)), aes(shape=has_glm)) +
    theme_bw() +
    scale_x_log10('Minimum Observation Dates', breaks=rep(c(1,2,5), times=4)*rep(c(1,10,100,1000), each=3), minor_breaks=NULL) +
    scale_y_log10('Number of Lakes Meeting Criteria', breaks=rep(c(1,2,5), times=4)*rep(c(1,10,100,1000), each=3), minor_breaks=NULL) +
    scale_color_discrete('Min Depths per Date', breaks=obs_thresholds) +
    scale_linetype_discrete('Has GLM Params') +
    scale_shape_discrete('Has GLM Params')

  if(!dir.exists(dirname(out_file))) dir.create(dirname(out_file))
  ggsave(out_file, width=7, height=6)
}
