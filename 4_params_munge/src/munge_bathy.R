
munge_wbic_bathy <- function(out_ind, bathy_zip_ind, wbic_xwalk_ind){

  wbic_xwalk <- sc_retrieve(wbic_xwalk_ind) %>% readRDS()
  bth_dir <- tempdir()
  bathy_data <- sc_retrieve(bathy_zip_ind) %>% unzip(exdir = bth_dir) %>%
    purrr::map(function(x) {
      WBIC_ID <- basename(x) %>% str_extract('WBIC_[0-9]+')
      read_tsv(x, col_types = 'dd') %>% mutate(WBIC_ID = WBIC_ID)
    }) %>% purrr::reduce(rbind) %>%
    inner_join(wbic_xwalk, by = 'WBIC_ID') %>% dplyr::select(site_id, depths = depth, areas = area)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(bathy_data, data_file)
  gd_put(out_ind, data_file)
}

munge_mndow_bathy <- function(out_ind, bathy_zip_ind, mndow_xwalk_ind){

  mndow_xwalk <- readRDS(sc_retrieve(mndow_xwalk_ind))

  bth_dir <- tempdir()


  # this still has the state-specific ID, since some NHDHR ids (site_id) have more than one. We need to combine those later
  bathy_data_mapped <- sc_retrieve(bathy_zip_ind) %>% unzip(exdir = bth_dir) %>%
    purrr::map(function(x) {
      MNDOW_ID <- basename(x) %>% str_extract('_[0-9]+.csv$') %>% str_extract('[0-9]+') %>% paste0('mndow_', .)
      read_csv(x, col_types = 'dd') %>% mutate(MNDOW_ID = MNDOW_ID)
    }) %>% purrr::reduce(rbind) %>%
    inner_join(mndow_xwalk, by = 'MNDOW_ID')
  browser()
  # now time to combine
  bathy_data <- purrr::map(bathy_data_mapped$site_id, function(x){

    this_bathy <- filter(bathy_data_mapped, site_id == x)
    state_ids <- unique(this_bathy$MNDOW_ID)
    if (length(state_ids) > 1){
      z_all <- round(this_bathy$depths, 1) %>% unique() %>% sort
      purrr::map(state_ids, function(id){
        ._d <- filter(this_bathy, MNDOW_ID == id)
        data.frame(site_id = x,
                   areas = approx(xout = z_all, x = ._d$depths, y = ._d$areas, rule = 2:1)$y, depths = z_all, stringsAsFactors = FALSE)
      }) %>% reduce(rbind) %>%
        group_by(depths, site_id) %>% summarize(areas = sum(areas, na.rm = TRUE))
    } else {
      this_bathy
    }

  }) %>% purrr::reduce(rbind) %>% dplyr::select(site_id, depths, areas)


  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(bathy_dat, data_file)
  gd_put(out_ind, data_file)
}
