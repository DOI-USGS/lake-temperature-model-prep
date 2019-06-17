crosswalk_coop_dat <- function(outind = target_name,
                               inind = '7a_temp_coop_munge/tmp/all_coop_dat.rds.ind',
                               id_crosswalk, wbic_crosswalk, dow_crosswalk) {

  infile <- scipiper::sc_retrieve(inind)
  outfile <- as_data_file(outind)
  dat <- readRDS(infile)

  idfile <- sc_retrieve(id_crosswalk, remake_file = '2_crosswalk_munge.yml')
  id2nhd <- readRDS(idfile)
  id2nhd <- rename(id2nhd, site_id = id)
  id2nhd <- distinct(id2nhd)

  wbicfile <- sc_retrieve(wbic_crosswalk, remake_file = '1_crosswalk_fetch.yml')
  wbic2nhd <- get(load(wbicfile))
  wbic2nhd$WBIC <- as.character(wbic2nhd$WBIC)
  wbic2nhd <- distinct(wbic2nhd) # get rid of duplicated obs


  dowfile <- sc_retrieve(dow_crosswalk, remake_file = '1_crosswalk_fetch.yml')
  dow2nhd <- get(load(dowfile))
  dow2nhd$DOW <- as.character(dow2nhd$dowlknum)
  dow2nhd <- distinct(dow2nhd) # get rid of duplicated obs

  # first, remove any

  # merge each possible ID with nhdid
  # wbic
  wbics_in_dat <- wbic2nhd %>%
    filter(WBIC %in% unique(dat$WBIC)) %>%
    distinct() %>%
    group_by(WBIC) %>%
    summarize(site_id = first(site_id))

  # dupes <- which(duplicated(wbics_in_dat$WBIC))

  dat_wbic <- filter(dat, !is.na(WBIC)) %>%
    left_join(wbics_in_dat)

  # dow
  dows_in_dat <- dow2nhd %>%
    filter(DOW %in% unique(dat$DOW)) %>%
    distinct() %>%
    dplyr::select(-dowlknum) %>%
    group_by(DOW) %>%
    summarize(site_id = first(site_id)) # this is a bandaid for multiple site matches

  # dupes <- which(duplicated(dows_in_dat$DOW))

  dat_dow <- filter(dat, !is.na(DOW)) %>%
    left_join(dows_in_dat)

  # id
  dat_id <- filter(dat, !is.na(id)) %>%
    left_join(id2nhd, by = c('id' = 'micoorps_id'))

  # all together now
  # print out warning about what data you're dropping
  dat_all_linked <- bind_rows(dat_wbic, dat_dow, dat_id) %>%
    tidyr::gather(key = state_id_type, value = state_id, DOW, id, WBIC) %>%
    filter(!is.na(state_id))

  warning(paste0('Dropping ', sum(is.na(dat_all_linked$site_id)), ' temperature observations due to missing NHD ids.'))

  dat_all_linked <- filter(dat_all_linked, !is.na(site_id)) %>%
    distinct()

  feather::write_feather(dat_all_linked, outfile)
  gd_put(outind, outfile)

}
