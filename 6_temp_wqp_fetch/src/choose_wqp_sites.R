#' Get a full list of sites for which we'll pull WQP data. Get this list from
#' the feature_crosswalk
choose_wqp_lakes <- function(feature_crosswalk_ind) {
  feature_crosswalk <- readRDS(sc_retrieve(feature_crosswalk_ind))
  site_ids <- feature_crosswalk %>%
    filter(!is.na(MonitoringLocationIdentifier)) %>%
    pull(site_id) %>%
    as.character() %>%
    unique()

  # temporarily restrict to a subset of site_ids for testing
  return(site_ids[1:148])
}

#' Use the crosswalk to go from raw (collaborator or NHD Perm ID) lake IDs to
#' WQP IDs with NHD IDs attached for grouping by lake
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


