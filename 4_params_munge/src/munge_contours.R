
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


munge_iadnr_bathy <- function(out_ind, iadnr_contour_ind, iadnr_surface_ind, xwalk_ind){

  iadnr_surface <- sc_retrieve(iadnr_surface_ind) %>% readRDS %>%
    mutate(depths = 0) %>% mutate(areas = as.numeric(st_area(st_polygonize(geometry)))) %>%
    st_drop_geometry()

  # none of these have a zero depth, so adding it as the min contour
  iadnr_contour <- sc_retrieve(iadnr_contour_ind) %>% readRDS %>%
    rename(depths = CONTOUR) %>%
    mutate(areas = as.numeric(st_area(geometry))) %>%
    st_cast("MULTILINESTRING") %>%
    rbind(iadnr_surface) %>%

    arrange(site_id, depth)



  browser()
  sf::st_read(shp.path, layer = layer, stringsAsFactors=FALSE) %>%
    mutate(site_id = paste0('iadnr_', lakeCode)) %>%
    group_by(site_id) %>% slice(which.min(CONTOUR)) %>%
    dplyr::select(site_id, geometry) %>% ungroup() %>%
    st_transform(x, crs = 4326)

  xwalk <- sc_retrieve(xwalk_ind) %>% readRDS()
}
