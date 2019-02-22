merge_id_crosswalks <- function(
  merged_ind,
  lakes_sf_ind = '1_crosswalk_fetch/out/lakes_sf.rds.ind',
  wqp_nhd_ind = '2_crosswalk_munge/out/wqp_nhdLookup.rds.ind'
) {

  lakes_sf <- readRDS(sc_retrieve(lakes_sf_ind))
  wqp_nhd <- readRDS(sc_retrieve(wqp_nhd_ind))

  # join such that we end up with all lakes listed in model_lakes.shp and only
  # those sites from wqp_nhdLookup.rds that have a matching lake polygon (where
  # match is based on nhd id with nhd_ prefix)
  merged <- left_join(
    mutate(lakes_sf, site_id=as.character(site_id)),
    mutate(wqp_nhd, id=as.character(id)),
    by=c(site_id='id'))

  data_file <- scipiper::as_data_file(merged_ind)
  saveRDS(merged, data_file)
  gd_put(merged_ind, data_file)
}
