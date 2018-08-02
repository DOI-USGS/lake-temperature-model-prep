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
wqp_calc_haves <- function(haves_ind, haves_dir, archive_ind) {

  # read in the partitions archive file for info on which files map to which sites
  archive <- feather::read_feather(sc_retrieve(archive_ind))

  # identify which files we have based on the presence of .feather.ind files in
  # the out directory, then join that file info with site info
  all_haves <-
    data_frame(
      PullFile=dir(haves_dir, pattern='*\\.feather\\.ind$', full.names=FALSE),
      PullTask=tools::file_path_sans_ext(basename(scipiper::as_data_file(PullFile)))) %>% # requires scipiper 0.0.12+
    left_join(archive, by='PullTask')

  # error checking - we should always have a record of every pulled file in the
  # archive, so if we don't, we need to fix something right now before we let
  # more critical info slide away
  if(any(!complete.cases(all_haves))) {
    na_files <- all_haves %>% filter(!complete.cases(.)) %>% pull(PullFile) %>% file.path(haves_dir, .)
    warning(sprintf('In wqp_calc_haves, these files have no match in the partitions archive:\n%s', paste0("  ", na_files, collapse='\n')))
  }

  # add value to the haves data by filtering down to just the most recent file
  # for each site - this gives us a final mapping of lakes to files that can be
  # used to prepare final data for the models
  final_haves <- all_haves %>%
    group_by(site_id) %>%
    filter(PullDate == max(PullDate))

  # write and upload the data file
  feather::write_feather(final_haves, path=scipiper::as_data_file(haves_ind))
  gd_put(haves_ind) # 1-arg version requires scipiper 0.0.11+
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
