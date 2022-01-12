merge_id_crosswalks <- function(
  merged_ind,
  lakes_sf_ind,
  wqp_nhd_ind) {

  lakes_sf <- readRDS(sc_retrieve(lakes_sf_ind))
  wqp_nhd <- readRDS(sc_retrieve(wqp_nhd_ind))

  # join such that we end up with all lakes listed in model_lakes.shp and only
  # those sites from wqp_nhdLookup.rds that have a matching lake polygon (where
  # match is based on nhd id with nhd_ prefix)
  merged <- left_join(
    mutate(lakes_sf, site_id=as.character(site_id)),
    mutate(wqp_nhd, site_id=as.character(site_id)),
    by=c('site_id'))

  data_file <- scipiper::as_data_file(merged_ind)
  saveRDS(merged, data_file)
  gd_put(merged_ind, data_file)
}




#' @param depth_sf_ind indicator file for Navico depths sf file
#' @param temp_ind indicator file for Navico water temperature lakes
#'
#' @details to create a full Navico crosswalk we need information from both of
#' these files. Including `temp_ind` is a bit odd, because it exists in the coop
#' fetch step and not all contributors to this pipeline have built that
#'
#' This function will error if the `sc_retrieve` for temp_ind doesn't have
#' access to 6_temp_coop_fetch_tasks.yml, which isn't checked into git

merge_navico_lakes <- function(out_ind, depth_sf_ind, temp_ind) {

  if (!file.exists('6_temp_coop_fetch_tasks.yml')){
    stop('in order to access ', temp_ind, ' must have built 6_temp_coop_fetch_tasks.yml', call. = FALSE)
  }
  outfile <- as_data_file(out_ind)

  navico_depth_points_sf <- scipiper::sc_retrieve(depth_sf_ind) %>%
    readRDS() %>% dplyr::select(site_id)

  navico_temp_file <- scipiper::sc_retrieve(temp_ind, remake_file = '6_temp_coop_fetch_tasks.yml')
  # temp doesn't use the default location for `gd_get()` for this target, which would be `getters.yml`,
  # so setting this arg to the task table yaml:
  navico_temp_points_sf <- readxl::excel_sheets(navico_temp_file) %>%
    lapply(function(x){
      read_xlsx(navico_temp_file, sheet = x, col_types = c(rep('text', 2), # state and wb name
                                                           rep('numeric', 18), # all data columns and some metadata
                                                           rep('date', 2)))
    }) %>%
    bind_rows() %>%
    filter(!is.na(MapWaterbodyId), !is.na(WaterbodyLon), !is.na(WaterbodyLat)) %>% group_by(MapWaterbodyId) %>%
    summarize(
      site_id = sprintf("Navico_%s", dplyr::first(MapWaterbodyId)),
      lat = dplyr::first(WaterbodyLat),
      lon = dplyr::first(WaterbodyLon)) %>%
    dplyr::select(site_id, lon, lat) %>%
    st_as_sf(coords = c('lon','lat'), crs=4326)

  bind_rows(navico_temp_points_sf, navico_depth_points_sf) %>%
    group_by(site_id) %>% summarize(
      # when lakes are in both, use the location of the depth file, which is more likely to be in a central location:
      site_id = dplyr::last(site_id)) %>%
    saveRDS(file = outfile)

  gd_put(out_ind, outfile)
}
