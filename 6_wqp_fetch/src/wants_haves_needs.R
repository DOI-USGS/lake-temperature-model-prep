#' Calculate your WQP "wants" using a vector of sites and variables to create a
#' dataframe of all site/variable combinations.
#'
#' @param wqp_sites A vector of all site IDs from which you would like to have
#'   WQP data.
#' @param wqp_variables List of lists that contain parameters of interest (e.g.,
#'   temperature) and all corresponding synonyms available in WQP (e.g.,
#'   "Temperature" and "Temperature, water").
#' @return A dataframe with columns "site" and "variable" that captures all
#'   possible combinations of sites and variables of interest.
wqp_calc_needs <- function(wants_ind, wqp_variables, wqp_xwalk_ind) {
  wqp_sites <- sc_retrieve(wqp_xwalk_ind) %>% readRDS
  df <- expand.grid(
    MonitoringLocationIdentifier=as.character(wqp_sites$MonitoringLocationIdentifier),
    ParamGroup=names(wqp_variables),
    stringsAsFactors=FALSE) %>%
    as_data_frame() %>%
    left_join(wqp_sites, by='MonitoringLocationIdentifier')

  # write and indicate the data file
  data_file <- scipiper::as_data_file(wants_ind)
  feather::write_feather(df, data_file)
  sc_indicate(wants_ind, data_file=data_file)
}

