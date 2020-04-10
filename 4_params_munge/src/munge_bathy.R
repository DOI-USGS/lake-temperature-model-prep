
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

munge_ndgf_bathy <- function(out_ind, ndgf_contour_ind, ndgf_xwalk_ind, ndgf_surface_sf_ind, crs){

  ndgf_xwalk <- readRDS(sc_retrieve(ndgf_xwalk_ind))

  # Now crosswalk with NHD lake ids
  ndgf_contours <- readRDS(sc_retrieve(ndgf_contour_ind)) %>%
    mutate(NDGF_ID = sprintf("ndgf_%s", LAKE)) %>%
    inner_join(ndgf_xwalk, by = 'NDGF_ID') %>%
    rename(other_ID = NDGF_ID,
           depths = CONTOUR) %>%
    dplyr::select(site_id, other_ID, depths)

  # Due to some issues with incomplete surface polygons in the contour data
  #   for some lakes causing zero area lakes and ultimately monotonicity issues,
  #   replace all contour shapes (depth = 0) with the polygons from ndgf_lakesvw.

  # We will treat these separately until after issues with some of the contour data are resolved

  # Many of the lakes have multiple polygons at the same depth. Some of the smaller
  # polygons at the same depth are contained within a larger polygon at the same depth.
  # This doesn't make much sense and we end up double counting the area. So, union them
  # first to reconcile internal boundaries by lake and then find the area.

  ndgf_non_surface_geoms <- ndgf_contours %>%
    filter(depths != 0) %>%
    group_by(site_id, other_ID, depths) %>%
    summarize(geometry = st_union(geometry)) %>%
    mutate(areas = as.numeric(st_area(st_polygonize(geometry)))) %>%
    dplyr::select(site_id, other_ID, depths, areas) %>%
    ungroup() %>%
    st_transform(crs = crs)

  # Now isolate the surface geometries for the lakes in the contour dataset
  # Make sure to join the crosswalk
  ndgf_surface_geoms <- readRDS(sc_retrieve(ndgf_surface_sf_ind)) %>%
    rename(NDGF_ID = site_id) %>%
    left_join(ndgf_xwalk) %>%
    filter(NDGF_ID %in% unique(ndgf_non_surface_geoms$other_ID)) %>%
    mutate(depths = 0, areas = as.numeric(st_area(geometry))) %>%
    # Needs to be in the same order as non surface one because we are binding rows
    dplyr::select(site_id, other_ID = NDGF_ID, depths, areas) %>%
    # Convert geometry type so it matches the type coming from contour data
    st_cast("MULTILINESTRING") %>%
    st_transform(crs = crs)

  # Combine the surface only geoms with the non surface geoms
  #   assumes that they have the same columns in the same order
  ndgf_geoms <- rbind(ndgf_surface_geoms, ndgf_non_surface_geoms)

  # Then, format to get ready for next step
  ndgf_bathy <- ndgf_geoms %>%
    filter(!other_ID %in% "ndgf_516") %>% # This lake has some weirdness ... has both positive (not just 0) and negative depths
    # Bathy needs to be positive depth (assumes depth = "feet below surface") and
    #   starts from surface moving down (so arrange by increasing depth)
    mutate(depths = -depths) %>%
    arrange(depths)

  not_working<-c("752", "751", "753", "750", "077", "020","046",
                 "001", "551", "085", "305","646", "285")

  ndgf_bathy_valid <- ndgf_bathy %>%
    # There are still shapes with zero areas (and no other source of
    # shape information like we had with the surface depths), so we
    # filter them out so that they do not cause issues
    filter(areas != 0) %>%
    # IDs stored in `not_working` are not passing monotonicity
    # Still investigating why that is the issue. Might be because
    # they had both positive and negative depths. Maybe do filter
    # areas > depth=0 as a start?
    filter(!other_ID %in% sprintf("ndgf_%s", not_working))

  data_file <- scipiper::as_data_file(out_ind)
  collapse_multi_bathy(ndgf_bathy_valid) %>%
    saveRDS(data_file)
  gd_put(out_ind, data_file)
}

