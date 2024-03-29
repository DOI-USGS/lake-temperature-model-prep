list_coop_files <- function(fileout, dirpath, trigger_file){
  make_file_stale(trigger_file)
  scipiper::sc_indicate(fileout,  data_file = list.files(dirpath, full.names = TRUE))
}


crosswalk_coop_dat <- function(outind = target_name, inind,
                               id_crosswalk, wbic_crosswalk,
                               dow_crosswalk, iowa_crosswalk,
                               missouri_crosswalk, mo_usace_crosswalk,
                               navico_crosswalk, norfork_crosswalk,
                               underc_crosswalk, sd_crosswalk,
                               in_clp_crosswalk, in_dnr_crosswalk) {

  outfile <- as_data_file(outind)

  # modify DOWs to add leading zero if not 8 characters long
  dat <- merge_coop_dat(inind)

  idfile <- sc_retrieve(id_crosswalk)
  id2nhd <- readRDS(idfile)
  id2nhd <- rename(id2nhd, site_id = id)
  id2nhd <- distinct(id2nhd)

  wbicfile <- sc_retrieve(wbic_crosswalk)
  wbic2nhd <- readRDS(wbicfile)
  wbic2nhd$WBIC <- gsub('WBIC_', '', as.character(wbic2nhd$WBIC_ID))
  wbic2nhd <- distinct(wbic2nhd) # get rid of duplicated obs


  dowfile <- sc_retrieve(dow_crosswalk)
  dow2nhd <- readRDS(dowfile)
  dow2nhd$DOW <- gsub('mndow_', '', as.character(dow2nhd$MNDOW_ID))
  dow2nhd <- distinct(dow2nhd) # get rid of duplicated obs

  iowa2nhd <- sc_retrieve(iowa_crosswalk) %>% readRDS() %>% distinct()

  missouri2nhd <- sc_retrieve(missouri_crosswalk) %>% readRDS() %>% distinct()

  mousace2nhd <- sc_retrieve(mo_usace_crosswalk) %>% readRDS() %>% distinct()

  navico2nhd <- sc_retrieve(navico_crosswalk) %>% readRDS() %>% distinct()

  norfork2nhd <- sc_retrieve(norfork_crosswalk) %>% readRDS() %>% distinct()

  underc2nhd <- sc_retrieve(underc_crosswalk) %>% readRDS() %>%
    # this crosswalk has a WBIC field with some lakes, this isn't what
    # we're using the for crosswalk (was point/poly instead)
    dplyr::select(site_id, UNDERC_ID) %>% distinct()

  sd2nhd <- sc_retrieve(sd_crosswalk) %>% readRDS() %>% distinct()

  in_clp2nhd <- sc_retrieve(in_clp_crosswalk) %>% readRDS() %>% distinct() %>%
    dplyr::select(site_id, IN_CLP_ID)

  in_dnr2nhd <- sc_retrieve(in_dnr_crosswalk) %>% readRDS() %>% distinct() %>%
    dplyr::select(site_id, IN_DNR_ID)

  # merge each possible ID with nhdid
  # wbic
  wbics_in_dat <- wbic2nhd %>%
    filter(WBIC %in% unique(dat$WBIC)) %>%
    distinct() %>%
    group_by(WBIC) %>%
    dplyr::summarize(site_id = first(site_id))

  # dupes <- which(duplicated(wbics_in_dat$WBIC))

  dat_wbic <- filter(dat, !is.na(WBIC)) %>%
    left_join(wbics_in_dat)

  # iowa
  dat_iowa <- filter(dat, !is.na(Iowa_ID)) %>%
    left_join(iowa2nhd)

  # dow
  dat_filt_dow <- filter(dat, !is.na(DOW)) %>%
    filter(!DOW == '') %>%
    mutate(DOW = ifelse(nchar(DOW)==8, DOW, paste0('0', DOW)))

  dows_in_dat <- dow2nhd %>%
    filter(DOW %in% unique(dat_filt_dow$DOW)) %>%
    distinct() %>%
    group_by(DOW) %>%
    dplyr::summarize(site_id = first(site_id)) # this is a bandaid for multiple site matches

  # dupes <- which(duplicated(dows_in_dat$DOW))

  dat_dow <- dat_filt_dow %>%
    left_join(dows_in_dat)

  # id
  dat_id <- filter(dat, !is.na(id)) %>%
    left_join(id2nhd, by = c('id' = 'micoorps_id'))

  # univ missouri
  dat_missouri <- filter(dat, !is.na(Missouri_ID)) %>%
    left_join(missouri2nhd)

  # missouri usace
  dat_mousace <- filter(dat, !is.na(mo_usace_id)) %>%
    left_join(mousace2nhd)

  # navico
  dat_navico <- filter(dat, !is.na(Navico_ID)) %>%
    left_join(navico2nhd)

  # norfork
  dat_norfork <- filter(dat, !is.na(Norfork_ID)) %>%
    left_join(norfork2nhd)

  # UNDERC
  dat_underc <- filter(dat, !is.na(UNDERC_ID)) %>%
    left_join(underc2nhd)

  # South Dakota
  dat_sd <- filter(dat, !is.na(SD_ID)) %>% left_join(sd2nhd)

  # Indiana CLP
  dat_in_clp <- filter(dat, !is.na(IN_CLP_ID)) %>% left_join(in_clp2nhd)

  # Indiana DNR
  dat_in_dnr <- filter(dat, !is.na(IN_DNR_ID)) %>% left_join(in_dnr2nhd)

  # all together now
  # print out warning about what data you're dropping
  dat_all_linked <- bind_rows(dat_wbic, dat_dow, dat_id, dat_iowa,
                              dat_missouri, dat_mousace,
                              dat_navico, dat_norfork,
                              dat_underc, dat_sd,
                              dat_in_clp, dat_in_dnr) %>%
    tidyr::pivot_longer(c(DOW, id, WBIC, Iowa_ID, Missouri_ID, mo_usace_id,
                          Navico_ID, Norfork_ID, UNDERC_ID, SD_ID, IN_CLP_ID,
                          IN_DNR_ID),
                        names_to = "state_id_type", values_to = "state_id") %>%
    filter(!is.na(state_id))

  # find which coop files have missing crosswalks
  dat_missing <- dat_all_linked %>%
    group_by(source) %>%
    dplyr::summarize(all_missing = all(is.na(site_id)),
              sum_missing = sum(is.na(site_id)))

  warning(paste0('Dropping ', sum(is.na(dat_all_linked$site_id)), ' temperature observations due to missing NHD ids.'))

  dat_all_linked <- filter(dat_all_linked, !is.na(site_id)) %>%
    distinct()

  feather::write_feather(dat_all_linked, outfile)
  gd_put(outind, outfile)

}
