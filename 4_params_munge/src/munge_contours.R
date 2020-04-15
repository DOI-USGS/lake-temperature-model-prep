
#' format for these outputs: site_id, {source}_ID, z_max
munge_ndgf_depths <- function(out_ind, ndgf_depths_ind, ndgf_xwalk_ind){

  ndgf_xwalk <- sc_retrieve(ndgf_xwalk_ind) %>% readRDS()

  ndgf_depths <- sc_retrieve(ndgf_depths_ind) %>% readRDS() %>%
    mutate(NDGF_ID = sprintf("ndgf_%s", LAKE), depth = DEPTH,
           z_max = as.numeric(depth)) %>% filter(!is.na(z_max)) %>%
    inner_join(ndgf_xwalk, by = 'NDGF_ID') %>% dplyr::select(site_id, NDGF_ID, z_max)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(ndgf_depths, data_file)
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



munge_indnr_bathy <- function(out_ind, contour_zip_ind, layer, lake_surface_ind, NHDID_WATE_xwalk, LAKE_NAME_xwalk){
  data_file <- scipiper::as_data_file(out_ind)

  # had at least one depth where the contours weren't able to be cast as polygons....this is a lot
  bad_cast <- c("nhdhr_62691603", "nhdhr_62682699", "nhdhr_53461851", "nhdhr_90345054", "nhdhr_{28f2e7d4-ede1-46f3-9913-e7de0c9e1008}", "nhdhr_155471202",
                "nhdhr_155469853", "nhdhr_155291260", "nhdhr_53447663", "nhdhr_96568183", "nhdhr_154247560", "nhdhr_123165957", "nhdhr_152100834",
                "nhdhr_{b6e2754f-7918-48bb-b6c5-e426895ad360}", "nhdhr_78542897", "nhdhr_155351290", "nhdhr_155641189", "nhdhr_04549854-A785-4DB7-9FC1-A3DEEBB23C49",
                "nhdhr_155642807", "nhdhr_155642356","nhdhr_{1DDD22E2-1723-4447-B1A0-57E008ED2F28}")
  # I did monotonicity tests on each remaining site_id group and these failed. I didn't get into trying to isolate one or more contours that were responsible,
  # instead rejecting the whole lake and creating a new mega-issue as https://github.com/USGS-R/lake-temperature-model-prep/issues/167
  failed_mono <- c('nhdhr_{13c737fd-517c-4f34-b813-79a31e833683}','nhdhr_{DF5F8AC1-4FD7-4D6D-9827-8692F742E74F}','nhdhr_120021837','nhdhr_123165517','nhdhr_145222228',
                   'nhdhr_145222944','nhdhr_155351274','nhdhr_155471071','nhdhr_155472807','nhdhr_155472809','nhdhr_155641743','nhdhr_155641907','nhdhr_155642885',
                   'nhdhr_49306509','nhdhr_53454933','nhdhr_56132527','nhdhr_62688605','nhdhr_96565971')

  drop_ids <- c(bad_cast, failed_mono)

  lake_surface <- sc_retrieve(lake_surface_ind) %>% readRDS() %>%
    rename(geometry = Shape) %>% sf::st_zm() %>%
    mutate(depths = 0) %>% dplyr::select(site_id, depths, geometry)

  zip_file <- sc_retrieve(contour_zip_ind)

  names_tbl <- purrr::map(1:length(LAKE_NAME_xwalk), function(j) {
    tibble(LAKE_NAME = names(LAKE_NAME_xwalk)[j], site_id_a = LAKE_NAME_xwalk[j])
    }) %>% purrr::reduce(bind_rows)

  ids_tbl <- purrr::map(1:length(NHDID_WATE_xwalk), function(j) {
    tibble(NHDID_WATE = names(NHDID_WATE_xwalk)[j], site_id_b = NHDID_WATE_xwalk[j])
  }) %>% purrr::reduce(bind_rows)

  # this is a simple internal function to avoid binding a bunch of new rows to the
  # tibble since _most_ lakes in the `lake_surface` tbl are _not_ in this IN-specific dataset
  # this also keeps us from having to filter out lake IDs that only have surface area in the
  # hypso calculations
  filter_id_and_bind <- function(existing_tbl, to_bind){
    filtered_new <- filter(to_bind, site_id %in% unique(existing_tbl$site_id))
    filtered_existing<- filter(existing_tbl, site_id %in% unique(filtered_new$site_id))
    rbind(filtered_existing, filtered_new)
  }

  shp.path <- tempdir()
  unzip(zip_file, exdir = shp.path)

  #' still some bad geoms
  #' requires sf version of at least 0.8:
  sf::st_read(shp.path, layer = layer, stringsAsFactors=FALSE) %>%
    sf::st_zm() %>%
    mutate(site_id_c = paste0('nhdhr_', NHDID_WATE)) %>%
    left_join(names_tbl, by = 'LAKE_NAME') %>%
    left_join(ids_tbl, by = 'NHDID_WATE') %>%
    # we want to order the columns so that dplyr::coalesce takes the first non-NA column
    dplyr::select(site_id_a, site_id_b, site_id_c, everything()) %>%
    mutate(site_id = coalesce(site_id_a, site_id_b, site_id_c)) %>%
    dplyr::select(-site_id_a, -site_id_b, -site_id_c) %>%
    # remove all sites that don't have any matches
    mutate(depths = CONTOUR * 0.3048) %>% dplyr::select(site_id, geometry, depths) %>%
    # add the canonical lakes that already have IDs in this dataset:
    filter_id_and_bind(lake_surface) %>%
    filter(site_id != 'nhdhr_NA') %>%
    st_transform(crs = 4326) %>%
    # remove all IDs that have failed either of our tests that were combined above
    filter(!site_id %in% drop_ids) %>%
    mutate(geometry = st_cast(geometry, "MULTIPOLYGON")) %>%
    st_make_valid() %>%
    group_by(site_id, depths) %>%
    summarise(geometry = st_union(geometry), areas = sum(as.numeric(st_area(geometry)))) %>%
    # there is no "other_id" in this case, but want to check monotonicity
    st_drop_geometry() %>% mutate(other_ID = site_id) %>%
    ungroup() %>%
    collapse_multi_bathy() %>%
    saveRDS(data_file)
  message('warning, dropping ', length(drop_ids), ' from this hypsography dataset')
  gd_put(out_ind, data_file)
}



munge_iadnr_bathy <- function(out_ind, iadnr_contour_ind, iadnr_surface_ind, xwalk_ind){

  xwalk <- sc_retrieve(xwalk_ind) %>% readRDS()

  iadnr_surface <- sc_retrieve(iadnr_surface_ind) %>% readRDS %>% group_by(site_id) %>%
    summarise(depths = 0, areas = sum(as.numeric(st_area(geometry)))) %>%
    st_drop_geometry()

  iadnr_contour <- sc_retrieve(iadnr_contour_ind) %>% readRDS %>%
    mutate(depths = CONTOUR * 0.3048) %>% group_by(site_id, depths) %>%
    summarise(areas = sum(as.numeric(st_area(geometry)))) %>%
    st_drop_geometry() %>%
    bind_rows(iadnr_surface) %>%
    arrange(site_id, depths) %>% rename(IADNR_ID = site_id) %>% ungroup() %>%
    left_join(xwalk, by = 'IADNR_ID') %>% filter(!is.na(site_id)) %>%
    rename(other_ID = IADNR_ID)


  data_file <- scipiper::as_data_file(out_ind)

  collapse_multi_bathy(iadnr_contour) %>%
    saveRDS(data_file)
  gd_put(out_ind, data_file)
}
