
crosswalk_points_in_poly <- function(ind_file, poly_ind_file, points_ind_file){
  poly_data <- gd_get(ind_file = poly_ind_file) %>% readRDS
  points_data <- gd_get(ind_file = points_ind_file) %>% readRDS

  stopifnot('site_id' %in% names(poly_data))
  stopifnot('MonitoringLocationIdentifier' %in% names(points_data))

  crosswalked_points <- st_join(points_data, poly_data, join = st_intersects) %>%
    filter(!is.na(site_id))

  crosswalked_ids <- data.frame(
    MonitoringLocationIdentifier = crosswalked_points$MonitoringLocationIdentifier,
    site_id = crosswalked_points$site_id)

  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(crosswalked_ids, data_file)
  gd_put(ind_file, data_file)
}

crosswalk_lagos_ids <- function(ind_file, poly_ind_file, ID_name){
  poly_data <- gd_get(ind_file = poly_ind_file) %>% readRDS
  browser()
  # need to get both sets of IDs from the shapefile...
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
