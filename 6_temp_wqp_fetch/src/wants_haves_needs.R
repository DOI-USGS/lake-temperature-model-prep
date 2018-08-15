#' Use the crosswalk to go from raw (collaborator) sites to
nhd_add_wqp_sites <- function(site_ids, feature_crosswalk_ind) {
  # read and filter the crosswalk to useful rows
  feature_crosswalk <- readRDS(sc_retrieve(feature_crosswalk_ind)) %>%
    filter(!is.na(MonitoringLocationIdentifier)) %>%
    mutate(MonitoringLocationIdentifier = as.character(MonitoringLocationIdentifier))

  # check for sites we've requested but can't have
  if(!all(site_ids %in% feature_crosswalk$site_id)) {
    missing_sites <- site_ids[which(!(site_ids %in% feature_crosswalk$site_id))]
    warning('Missing site[s] in crosswalk: ', paste0(missing_sites, collapse=', '))
  }

  # return a lookup table of site_ids and WQP IDs, leaving out any for which we
  # couldn't find a WQP ID
  data_frame(site_id=site_ids) %>%
    inner_join(feature_crosswalk, by='site_id') %>%
    select(site_id, MonitoringLocationIdentifier)
}


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
wqp_calc_wants <- function(wants_ind, wqp_sites, wqp_variables) {
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
#' This will not detect new needs due to an increase in the number of
#' temperature characteristicName, though it would detect the addition of an
#' entirely new ParamGroup. If you add a temperature characteristicName, force a
#' rebuild of the needs, inventory, and data files.
#'
#' @param wants A dataframe of all site/variable combinations of interest.
#' @param haves_ind Indicator file of an inventory of all site/variable
#'   combinations for which you already have data.
#' @return A dataframe that includes site/variable combinations which reflects
#'   site/variable combinations in wqp_wants that are missing in wqp_haves.
wqp_calc_needs <- function(needs_ind, wants_ind, haves_ind) {

  wants <- feather::read_feather(sc_retrieve(wants_ind))
  haves <- feather::read_feather(sc_retrieve(haves_ind))

  # leaves only the rows in wqp_wants that don't exist in wqp_haves
  # these are site/variable combinations where we have no data
  needs <- dplyr::anti_join(wants, haves,  by = c('site_id', 'MonitoringLocationIdentifier', 'ParamGroup'))

  # write and indicate the data file
  data_file <- scipiper::as_data_file(needs_ind)
  feather::write_feather(needs, data_file)
  sc_indicate(needs_ind, data_file=data_file)
}
