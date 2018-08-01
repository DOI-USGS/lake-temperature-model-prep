#' Calculate your WQP "wants" using a vector of sites and variables to create a
#' dataframe of all site/variable combinations.
#'
#' @param wqp_sites A vector of all site IDs from which you would like to have
#'   WQP data.
#' @param wqp_variables List of lists that contain parameters of interest (e.g.,
#'   temperature) and all corresponding synonyms available in NWIS (e.g.,
#'   "Temperature" and "Temperature, water").
#' @return A dataframe with columns "site" and "variable" that captures all
#'   possible combinations of sites and variables of interest.
wqp_calc_wants <- function(wqp_sites, wqp_variables) {

  vars <- names(wqp_variables$characteristicName)
  df <- expand.grid(site=wqp_sites, variable=vars, stringsAsFactors=FALSE) %>%
    mutate(filename=)
  return(df)
}

#' Calculate your WQP "haves" by counting site files in haves_dir
#'
#' This function is designed to be run repeatedly. We could have this function's
#' output depend on the contents of the haves_dir, but that leads to funny
#' remake warnings and would run obnoxiously often during a pull with restarts.
#' So instead you should call `scmake('haves', force=TRUE)` when you're ready
#' for an update.
#'
#' @param haves_ind the table to write
#' @param haves_dir the directory where the WQP data .ind files reside
wqp_calc_haves <- function(haves_ind, haves_dir) {

  haves <- data_frame(
    PullTask=tools::file_path_sans_ext(basename(dir(haves_dir, pattern='*\\.feather\\.ind$', full.names=FALSE)))
  ) %>% left_join(

  )

  # write and upload the data file
  data_file <- scipiper::as_data_file(haves_ind)
  readr::write_tsv(haves, path=data_file)
  gd_put(haves_ind, data_file)
}


#' Calculate your WQP "needs" by finding the difference between your "wants" and
#' "haves".
#'
#' @param wqp_wants A dataframe of all site/variable combinations of interest.
#' @param wqp_haves An inventory of all site/variable combinations for which you
#'   already have data.
#' @return A dataframe that includes site/variable combinations which reflects
#'   site/variable combinations in wqp_wants that are missing in wqp_haves.
wqp_calc_needs <- function(wqp_wants, wqp_haves) {

  haves <- readRDS(wqp_haves)

  # leaves only the rows in wqp_wants that don't exist in wqp_haves
  # these are site/variable combinations where we have no data
  diffed_cells <- dplyr::anti_join(wqp_wants, haves,  by = c('site', 'variable'))

  if (any(is.na(diffed_cells))){
    # shouldn't be any NAs, but if there are, throw an error
    stop('found NA(s) in NLDAS cell diff. Check ', wqp_haves, ' and `wq_wants` data')
  }

  # return the site/variable combinations what we need to pull
  return(diffed_cells)
}
