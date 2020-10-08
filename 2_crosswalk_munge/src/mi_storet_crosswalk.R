# crosswalk between wqp to MI sites
# appears the storet sites IDs have the lake #s in them,
# but don't have the preceeding org ID.
# Looks likely it is 21MICH as orgid.
# try loose match, do some checking to see if lakes match based on this loose match

munge_micorps_crosswalk <- function(out_ind, site_ind, wqp_nhd_ind, wqp_latlong_ind) {

  mi_storet_ids <- read_excel(sc_retrieve(site_ind), sheet = 'oxygen_temp')
  wqp_nhdLookup <- readRDS(sc_retrieve(wqp_nhd_ind)) %>%
    distinct()
  wqp_latlong <- readRDS(sc_retrieve(wqp_latlong_ind))

  latlong <- st_coordinates(wqp_latlong) %>% as.data.frame() %>%
    mutate(MonitoringLocationIdentifier = wqp_latlong$site_id) %>%
    rename(LongitudeMeasure = X, LatitudeMeasure = Y) %>%
    distinct()

  wqp_nhdLookup <- left_join(wqp_nhdLookup, latlong, by = "MonitoringLocationIdentifier") %>%
    rename(id = site_id)


  source_ids <- as.character(unique(mi_storet_ids$STORETID))

  potential_sites <- dplyr::filter(wqp_nhdLookup, grepl(paste(source_ids, collapse = '|'), MonitoringLocationIdentifier))
  potential_sites <- dplyr::filter(potential_sites, !grepl('USGS', MonitoringLocationIdentifier))
  potential_sites$reduced_site_id <- gsub('(^.+)(-)(\\d+)', '\\3', potential_sites$MonitoringLocationIdentifier)

  mi_sites <- dplyr::select(mi_storet_ids, lakename = `Lake Name`, mi_id = STORETID,
                            mi_lat = Latitude, mi_long = Longitude) %>%
    dplyr::distinct()

  #stop('I must have broken something by removing lat/lon from the wqp lookup...')
  potential_matches <- left_join(potential_sites, mi_sites, by = c('reduced_site_id' = 'mi_id')) %>%
    mutate(lat_diff = abs(round(LatitudeMeasure, 4) - round(as.numeric(mi_lat), 4)),
           long_diff = abs(round(LongitudeMeasure, 4) - round(as.numeric(mi_long), 4)))

  # filter to near perfect lat/long match
  matches <- filter(potential_matches, lat_diff <= 0.05 & long_diff <= 0.05) %>%
    dplyr::select(reduced_site_id, id) %>%
    distinct() %>%
    rename(micoorps_id = reduced_site_id) %>%
    filter(!is.na(id))

  # note this does not capture all site IDs from micorps spreadsheet, but gets us most of the way there

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(matches, data_file)
  gd_put(out_ind, data_file)
}






# hand check these matches
# manually checked lat/long differences of 0.002-0.05,
# if you google map the lat/long from wqp crosswalk, end up on
# lake with same name from micoorps data
