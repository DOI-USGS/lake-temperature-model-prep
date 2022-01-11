
crosswalk_points_in_poly <- function(ind_file, poly_ind_file, points_ind_file, points_ID_name){

  # we now use sf > v1.0, which causes issues with some NHD polygons and the spherical coordinates
  # see info on the change: https://r-spatial.org/r/2020/06/17/s2.html#sf-10-goodbye-flat-earth-welcome-s2-spherical-geometry

  # to avoid these NHD issues, we'd either need to edit the files or avoid the error by avoiding use of the
  # S2 engine. We do that by toggling off use of S2 here.
  sf::sf_use_s2(FALSE)
  on.exit(sf::sf_use_s2(TRUE))
  poly_data <- gd_get(ind_file = poly_ind_file) %>% readRDS

  points_data <- gd_get(ind_file = points_ind_file) %>% readRDS %>%
    rename(!!points_ID_name := "site_id")

  stopifnot('site_id' %in% names(poly_data))


  crosswalked_points <- st_join(points_data, poly_data, join = st_intersects) %>%
    filter(!is.na(site_id))

  crosswalked_ids <- st_drop_geometry(crosswalked_points) %>%
    dplyr::select(site_id, !!points_ID_name, everything())

  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(crosswalked_ids, data_file)
  gd_put(ind_file, data_file)
}

crosswalk_poly_over_poly <- function(ind_file, poly1_ind_file, poly2_ind_file, poly1_ID_name){
  poly1_data <- readRDS(sc_retrieve(ind_file = poly1_ind_file))
  poly2_data <- readRDS(sc_retrieve(ind_file = poly2_ind_file))

  stopifnot('site_id' %in% names(poly1_data))
  stopifnot('site_id' %in% names(poly2_data))


  out <- data.frame(poly1_data$site_id, stringsAsFactors = FALSE) %>% setNames(poly1_ID_name) %>% mutate(site_id=NA_character_)

  poly2_sp <- as(st_zm(poly2_data), 'Spatial')
  pb <- txtProgressBar(min = 0, max = nrow(out), initial=0)

  for(i in 1:nrow(out)){
    tmp <- over(poly2_sp, as(st_zm(poly1_data[i,]), 'Spatial'))
    site_id <- poly2_sp$site_id[!is.na(tmp)] %>% as.character()
    if(!is.na(site_id[1])){
      if (length(site_id) > 1){
        subset_polys <- poly2_data[!is.na(tmp), ]
        subset_polys$starea <- as.numeric(sf::st_area(subset_polys))
        id <- arrange(subset_polys, desc(starea)) %>% pull(site_id) %>% as.character() %>% head(1)
      } else {
        id <- site_id[1]
      }

      out$site_id[i] <- id
    }
    setTxtProgressBar(pb, i)
  }

  crosswalk_out <- filter(out, !is.na(site_id))

  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(crosswalk_out, data_file)
  gd_put(ind_file, data_file)
}


crosswalk_poly_intersect_poly <- function(ind_file, poly1_ind_file, poly2_ind_file, poly1_ID_name, crs){

  sf::sf_use_s2(FALSE)
  on.exit(sf::sf_use_s2(TRUE))
  poly1_data <- readRDS(sc_retrieve(ind_file = poly1_ind_file))
  poly2_data <- readRDS(sc_retrieve(ind_file = poly2_ind_file))

  stopifnot('site_id' %in% names(poly1_data))
  stopifnot('site_id' %in% names(poly2_data))

  poly2_data <- st_zm(poly2_data)

  poly1_data <- st_transform(poly1_data, crs = crs)
  poly2_data <- st_transform(poly2_data, crs = crs)

  # Aggregate polys by site ID (this step likely unnecessary for NHD but keeping for generality)
  poly1_agg <- poly1_data %>%
    group_by(site_id) %>%
    summarise()

  poly2_agg <- poly2_data %>%
    group_by(site_id) %>%
    summarise()

  # Measure areas
  poly1_agg$poly1_Area <- as.numeric(st_area(poly1_agg))
  poly2_agg$site_id_Area <- as.numeric(st_area(poly2_agg))

  poly1_agg <- sf::st_buffer(poly1_agg, dist = 0)
  poly2_agg <- sf::st_buffer(poly2_agg, dist = 0)

  colnames(poly1_agg)[colnames(poly1_agg)=="site_id"] <- poly1_ID_name

  intersect.sf <-  sf::st_intersection(poly1_agg, poly2_agg)
  intersect.sf$Intersect_Area <- as.numeric(st_area(intersect.sf))

  ### Drop intersections that are <1% of poly1 area or poly2 area
  intersect.sf <- intersect.sf %>%
    filter(Intersect_Area > .01*poly1_Area) %>%
    filter(Intersect_Area > .01*site_id_Area)

  intersect_out <- intersect.sf %>%
    st_drop_geometry()

  colnames(intersect_out)[colnames(intersect_out)=="poly1_Area"] <- paste0(poly1_ID_name, "_Area")


  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(intersect_out, data_file)
  gd_put(ind_file, data_file)

}


choose1_poly_intersect_poly <- function(ind_file, intersect_ind_file, poly1_ID_name){

  intersect_data <- readRDS(sc_retrieve(ind_file = intersect_ind_file))

  stopifnot(paste0(poly1_ID_name) %in% names(intersect_data))
  stopifnot('site_id' %in% names(intersect_data))
  stopifnot('Intersect_Area' %in% names(intersect_data))

  colnames(intersect_data)[colnames(intersect_data)==paste0(poly1_ID_name)] <- "Temp_ID_Name"

  intersect_data <- intersect_data %>% dplyr::select(Temp_ID_Name, site_id, Intersect_Area)

  crosswalk_out <- intersect_data %>% group_by(Temp_ID_Name) %>%
    arrange(desc(Intersect_Area)) %>%
    summarise(site_id = first(site_id))

  colnames(crosswalk_out)[colnames(crosswalk_out)=="Temp_ID_Name"] <- paste0(poly1_ID_name)


  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(crosswalk_out, data_file)
  gd_put(ind_file, data_file)

}



combine_sf_lakes <- function(out_ind, ...){
  sf_inds <- c(...)

  sf_lakes <- readRDS(scipiper::sc_retrieve(sf_inds[1L]))
  for (sf_ind in tail(sf_inds, -1)){
    sf_lakes <- rbind(sf_lakes, readRDS(scipiper::sc_retrieve(sf_ind)))
  }
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(sf_lakes, data_file)
  gd_put(out_ind, data_file)
}

centroid_sf_lakes <- function(out_ind, lake_ind){
  sf::sf_use_s2(FALSE)
  on.exit(sf::sf_use_s2(TRUE))
  sf_lakes <- readRDS(scipiper::sc_retrieve(lake_ind))
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(st_centroid(sf_lakes), data_file)
  gd_put(out_ind, data_file)

}


buffer_sf_lakes <- function(out_ind, lake_ind, buffer_width){
  sf_lakes <- readRDS(scipiper::sc_retrieve(lake_ind)) %>%
    st_transform(crs = "+proj=laea +lat_0=45 +lon_0=-100 +x_0=0 +y_0=0 +a=6370997 +b=6370997 +units=m +no_defs")

  sf_buffered_lakes <- st_buffer(sf_lakes, dist = buffer_width)

  sf_donut_lakes <- sf_lakes
  for (j in 1:nrow(sf_donut_lakes)){
    sf_donut_lakes[j, ] <- st_difference(sf_buffered_lakes[j, ], sf_lakes[j, ]) %>% dplyr::select(site_id, Shape)
  }
  sf_donut_lakes <- sf_donut_lakes %>%
    dplyr::select(site_id, Shape) %>%
    st_transform(crs = 4326)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(sf_donut_lakes, data_file)
  gd_put(out_ind, data_file)

}

us_counties_to_states <- function(ind_file, us_counties_ind) {
  us_counties_sf <- readRDS(sc_retrieve(us_counties_ind))
  conus_states <- group_by(us_counties_sf, state) %>%
    summarise() %>%
    st_transform(crs = 4326)

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(conus_states, data_file)
  gd_put(ind_file, data_file)
}

# Intersects lake polygons with state polygons to match each lake to
# one or more states. If the lake doesn't match any (e.g. it's in
# Canada), it is given an `NA` in the `state` column.
crosswalk_lakes_intersect_states <- function(ind_file, lakes_ind, states_ind) {

  lakes_sf <- readRDS(sc_retrieve(lakes_ind))
  states_sf <- readRDS(sc_retrieve(states_ind))

  crosswalked_polygons <- lakes_sf %>%
    st_make_valid() %>%
    st_zm() %>%
    st_join(st_make_valid(states_sf), join = st_intersects)

  crosswalked_ids <- crosswalked_polygons %>%
    st_drop_geometry() %>%
    dplyr::select(site_id, state)

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(crosswalked_ids, data_file)
  gd_put(ind_file, data_file)
}
