
munge_wbic_bathy <- function(out_ind, bathy_zip_ind, wbic_xwalk_ind){

  wbic_xwalk <- sc_retrieve(wbic_xwalk_ind) %>% readRDS()
  bth_dir <- tempdir()
  bathy_data <- sc_retrieve(bathy_zip_ind) %>% unzip(exdir = bth_dir) %>%
    purrr::map(function(x) {
      WBIC_ID <- basename(x) %>% str_extract('WBIC_[0-9]+')
      read_tsv(x, col_types = 'dd') %>% mutate(WBIC_ID = WBIC_ID)
    }) %>% purrr::reduce(rbind) %>%
    inner_join(wbic_xwalk, by = 'WBIC_ID') %>% dplyr::select(site_id, depths = depth, areas = area, other_ID = WBIC_ID)

  data_file <- scipiper::as_data_file(out_ind)
  collapse_multi_bathy(bathy_data) %>%
    saveRDS(data_file)
  gd_put(out_ind, data_file)
}

collapse_multi_bathy <- function(data_in){
  bathy_data <- purrr::map(unique(data_in$site_id), function(x){

    these_bathys <- filter(data_in, site_id == x)
    state_ids <- unique(these_bathys$other_ID)

    if (length(state_ids) > 1){ # will need to combine multiples:

      z_all <- round(these_bathys$depths, 1) %>% unique() %>% sort
      summarized_bathy <- purrr::map(state_ids, function(id){
        this_bathy <- filter(these_bathys, other_ID == id)
        data.frame(site_id = x,
                   areas = approx(xout = z_all, x = this_bathy$depths, y = this_bathy$areas, rule = 2:1)$y,
                   depths = z_all, stringsAsFactors = FALSE)
      }) %>% reduce(rbind) %>%
        group_by(depths, site_id) %>% dplyr::summarize(areas = sum(areas, na.rm = TRUE)) %>%
        dplyr::select(site_id, depths, areas)
    } else { # no need to combine
      summarized_bathy <- these_bathys %>% dplyr::select(site_id, depths, areas)
    }

    # check for monotonicity:
    fail_depths <- (!all(summarized_bathy$depths == cummax(summarized_bathy$depths)) | !all(summarized_bathy$areas == cummin(summarized_bathy$areas)))
    if (isTRUE(fail_depths)){
      stop(paste(state_ids, collapse = ', '), ' failed monotonicity test. NHDHR:', x)
    }

    return(summarized_bathy %>% filter(!is.na(areas)))
  }) %>% purrr::reduce(bind_rows)

}

munge_mndow_perc_bathy <- function(out_ind, bathy_zip_ind, mndow_xwalk_ind, mndow_poly_ind){


  lakes <- scipiper::sc_retrieve(mndow_poly_ind) %>% readRDS()
  # multiple matches to

  areas <- data.frame(MNDOW_ID = lakes$site_id, areas = as.numeric(st_area(lakes))) %>%
                        group_by(MNDOW_ID) %>% dplyr::summarize(areas_m2 = sum(areas), stringsAsFactors = FALSE)

  mndow_xwalk <- readRDS(sc_retrieve(mndow_xwalk_ind)) %>%
    left_join(areas, by = 'MNDOW_ID')

  bth_dir <- tempdir()

  # this still has the state-specific ID, since some NHDHR ids (site_id) have more than one. We need to combine those later
  bathy_data_mapped <- sc_retrieve(bathy_zip_ind) %>% unzip(exdir = bth_dir) %>%
    purrr::map(function(x) {
      MNDOW_ID <- basename(x) %>% str_extract('[0-9]+.csv$') %>% str_extract('[0-9]+') %>% paste0('mndow_', .)
      data <- {read_csv(x)[,1:2]} %>% setNames(c('depth_feet', 'proportion_area')) %>%
        mutate(MNDOW_ID = MNDOW_ID, depths = depth_feet * 0.3048) %>%
        dplyr::select(-depth_feet)
    }) %>% purrr::reduce(rbind) %>%
    inner_join(mndow_xwalk, by = 'MNDOW_ID') %>%
    mutate(areas = proportion_area * areas_m2) %>% rename(other_ID = MNDOW_ID) %>%
    dplyr::select(site_id, other_ID, depths, areas)

  data_file <- scipiper::as_data_file(out_ind)
  collapse_multi_bathy(bathy_data_mapped) %>%
    saveRDS(data_file)
  gd_put(out_ind, data_file)

}

# Get into format site_id, other_ID, depths, areas
munge_mndow_bathy <- function(out_ind, bathy_zip_ind, mndow_xwalk_ind){

  mndow_xwalk <- readRDS(sc_retrieve(mndow_xwalk_ind))

  bth_dir <- tempdir()
  # this still has the state-specific ID, since some NHDHR ids (site_id) have more than one. We need to combine those later
  bathy_data_mapped <- sc_retrieve(bathy_zip_ind) %>% unzip(exdir = bth_dir) %>%
    purrr::map(function(x) {
      MNDOW_ID <- basename(x) %>% str_extract('_[0-9]+.csv$') %>% str_extract('[0-9]+') %>% paste0('mndow_', .)
      read_csv(x, col_types = 'dd') %>% mutate(MNDOW_ID = MNDOW_ID)
    }) %>% purrr::reduce(rbind) %>%
    inner_join(mndow_xwalk, by = 'MNDOW_ID') %>% rename(other_ID = MNDOW_ID)


  data_file <- scipiper::as_data_file(out_ind)
  collapse_multi_bathy(bathy_data_mapped) %>%
    saveRDS(data_file)
  gd_put(out_ind, data_file)
}



