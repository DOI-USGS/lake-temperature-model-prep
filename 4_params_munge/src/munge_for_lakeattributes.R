munge_area <- function(out_ind, area_ind){

  #format for lakeattributes, which is column site_id and area_m2
  area_dat <- readRDS(sc_retrieve(area_ind)) %>%
    dplyr::select(-lake_name)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(area_dat, data_file)
  gd_put(out_ind, data_file)
}

munge_depth <- function(out_ind, lagos_depth_ind, lagos_crosswalk, mglp_depth_ind){

  lagos_depth_dat <- read.csv(sc_retrieve(lagos_depth_ind))
  lagos_cross <- readRDS(sc_retrieve(lagos_crosswalk))
  mglp_depth_dat <- readRDS(sc_retrieve(mglp_depth_ind))

  # bring depth data together and format for lakeattributes
  # columns are site_id, source, zmax_m
  lagos_clean <- lagos_depth_dat %>%
    mutate(LAGOS_ID = paste0('lagos_', lagoslakeid),
           source = 'lagos') %>%
    right_join(lagos_cross) %>%
    dplyr::select(site_id, source, zmax_m = zmaxobs)

  mglp_clean <- rename(mglp_depth_dat, zmax_m = z_max)

  depth_dat <- bind_rows(lagos_clean, mglp_clean) %>%
    distinct()

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(depth_dat, data_file)
  gd_put(out_ind, data_file)
}

munge_secchi <- function(out_ind, lagos_secchi_ind, lagos_crosswalk){

  lagos_secchi_dat <- readRDS(sc_retrieve(lagos_secchi_ind))
  lagos_cross <- readRDS(sc_retrieve(lagos_crosswalk))

  # format for lakeattributes
  # columns are site_id, year, date, secchi_m, source, datasource

  secchi_clean <- lagos_secchi_dat %>%
    rename(LAGOS_ID = site_id) %>%
    left_join(lagos_cross) %>%
    filter(!is.na(site_id)) %>%
    mutate(year = lubridate::year(date),
           source = 'in-situ') %>%
    dplyr::select(site_id, year, date, secchi_m = secchi, source, datasource = mon_group)



  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(secchi_clean, data_file)
  gd_put(out_ind, data_file)
}

munge_location <- function(out_ind, mglp_zip_ind, mglp_gdb_file, states){
  # columns for location are site_id, lon, lat

  zip_file <- scipiper::sc_retrieve(mglp_zip_ind)

  shp.path <- tempdir()
  unzip(zip_file, exdir = shp.path)

  shp <- sf::st_read(file.path(shp.path, mglp_gdb_file), layer = 'MGLP_LAKES')

  centroid <- shp %>%
    filter(ASSESS == 'Y', STATE %in% states) %>%
    mutate(centroid = st_centroid(SHAPE)) %>%
    mutate(centroid = st_transform(centroid, crs = 4326))

  latlong <- as.data.frame(st_coordinates(centroid$centroid))
  location <- data.frame(site_id = centroid$LAKE_ID,
                         lon = latlong$X,
                         lat = latlong$Y)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(location, data_file)
  gd_put(out_ind, data_file)
}

munge_canopyheight <- function(out_ind, nlcd_dat_ind, nlcd_heights_ind){

  nlcd_dat <- readRDS(sc_retrieve(nlcd_dat_ind))
  nlcd_heights <- readRDS(sc_retrieve(nlcd_heights_ind)) %>%
    mutate(class = as.character(class))

  #nlcd_classes <- readRDS(sc_retrieve(nlcd_class_ind))

  # first, find dominant classes per lakeid and merge with height
  # rename for lakeattributes, which needs site_id, canopy_m, source
  dom_class <- nlcd_dat %>%
    tidyr::gather(key = 'type', value = 'percent_in_buffer', -id) %>%
    group_by(id) %>%
    summarize(type = type[which.max(percent_in_buffer)]) %>%
    mutate(class = gsub('\\D*', '', type)) %>%
    left_join(dplyr::select(nlcd_heights, height_m, class)) %>%
    filter(!is.na(height_m)) %>%
    mutate(source = 'nlcd') %>%
    dplyr::select(site_id = id, canopy_m = height_m, source) %>%
    filter(grepl('mglp', site_id))

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(dom_class, data_file)
  gd_put(out_ind, data_file)
}
