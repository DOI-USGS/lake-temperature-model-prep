

fetch_sb_wfs <- function(ind_file, sb_id, layer){

  url <- 'https://www.sciencebase.gov/catalogMaps/mapping/ows/%s?service=wfs&request=GetFeature&typeName=sb:%s&outputFormat=shape-zip&version=1.0.0'
  destination = tempfile(pattern = 'lake_shape', fileext='.zip')
  query <- sprintf(url, sb_id, layer)
  file <- GET(query, write_disk(destination, overwrite=T), progress())
  shp.path <- tempdir()
  unzip(destination, exdir = shp.path)

  out <- sf::st_read(shp.path, layer=layer) %>%
    st_transform(crs = 4326) %>% dplyr::select(site_id, geometry)

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(out, data_file)
  gd_put(ind_file, data_file)

}

#' this function cheats and assumes a local file because there isn't a good webservice and the file is huge
#' see https://data-wi-dnr.opendata.arcgis.com/datasets/0128cce2c06342218725f1069031a4fa for WI hydrolayer
#' This function is used just to process the shapefile, and we use `gd_confirm_posted` after manually uploading to create the ind
process_wbic_lakes <- function(file_out = '1_crosswalk_fetch/out/wbic_lakes_sf.rds'){
  wbic_lakes <- sf::st_read('~/Downloads/24K_Hydro.gdb/', layer = 'WD_HYDRO_WATERBODY_WBIC_AR_24K') %>%
    mutate(site_id = paste0('WBIC_', WATERBODY_WBIC)) %>% dplyr::select(site_id, SHAPE) %>%
    rename(geometry = SHAPE) %>% # why do I need to rename SHAPE to geometry??
    sf::st_zm() %>%
    sf::st_transform(x, crs = 4326)

  saveRDS(wbic_lakes, file = file_out)
}

fetch_isro_lakes <- function(out_ind, zip_ind, gdb_filename, use_geoms){
  outfile <- as_data_file(out_ind)

  zip_file <- sc_retrieve(zip_ind)

  gdb_path <- tempdir()
  unzip(zip_file, exdir = gdb_path)


  st_layer_info <- st_layers(file.path(gdb_path, gdb_filename))
  layer_info <- tibble(layer_name = st_layer_info$name,
                       geom_type = unlist(st_layer_info$geomtype),
                       n_feature = st_layer_info$features,
                       n_col = st_layer_info$fields)

  # could replace this comment with sprintf for "use_geoms"
  message('...dropping any layer that is not Multi Polygon, likely more than five lost...')

  use_layers <- layer_info %>% filter(!is.na(geom_type) & n_feature > 1 & geom_type == use_geoms) %>%
  # all but one remaining have 8 cols (one has 9)
  # depth field is "Avg_Depth" for many even though that name is misleading, seems an int
  # or "Depth_calc" when 9 cols
    mutate(depth_col = case_when(
      n_col == 9 ~ "Depth_calc",
      n_col == 8 ~ "Avg_Depth"
    )) %>%
    dplyr::select(layer_name, depth_col)

  data_clean <- purrr::pmap(use_layers, function(layer_name, depth_col){
    # get "Beaver" from "Beaver_CntrPolys" etc:
    lake_name <- strsplit(layer_name, '_')[[1]][1]

    st_read(file.path(gdb_path, gdb_filename), layer_name, quiet = TRUE) %>%
      sf::st_zm() %>% st_transform(crs = 4326) %>%
      rename(geometry = Shape) %>%
      mutate(site_id = sprintf('isro_%s', lake_name)) %>%
      dplyr::select(site_id, m_depth = one_of(depth_col)) %>%
      arrange(m_depth) %>%
      group_by(site_id, m_depth) %>%
      # some depth intervals don't have much of a footprint, why?
      # e.g., filter(site_id == 'isro_Beaver' & m_depth == 1)
      summarize(geometry = sf::st_union(geometry), .groups = 'drop')
  }) %>% bind_rows() %>%
    saveRDS(file = outfile)

  gd_put(out_ind, outfile)

}

#' we're using this contour shapefile because the other lake shapefile doesn't really
#' have lake IDs, since most are empty. See issue here: https://github.com/USGS-R/lake-temperature-model-prep/issues/155
#' remove_IDs is needed because some of the contours have empty geometries for one or more contours.
#' Remove the whole lake in this case
#'
#'   #' see blodgett's code:
# # library(sf)
# # library(dplyr)
# l <- sf::read_sf("~/Downloads/test.gpkg") %>%
#   st_zm()
# g <- lapply(1:nrow(l), function(x, l) {
#
#   try(return(st_cast(st_geometry(l)[[x]], "MULTIPOLYGON")))
#
#   st_multipolygon()
# }, l = l)
# g <- st_sfc(g, crs = st_crs(l))
# st_geometry(l) <- g
# l[which(is.na(st_dimension(st_geometry(l)))),]$lakeCode %>% unique()
#' issue on row 1797; HEN45 CONTOUR ==12; 1799 HEN45 CONTOUR==15; 2500, LGR82, CONTOUR==34;
#' 2509; LGR82, CONTOUR==34 (another 34' contour); 2812; MIA68, CONTOUR==4
#' 2889; MOR59; CONTOUR==2
fetch_iadnr_lakes <- function(out_ind, zip_ind, remove_IDs, layer){

  outfile <- as_data_file(out_ind)

  zip_file <- sc_retrieve(zip_ind)
  #"iadnr_DBE43\r\nDBE43"
  shp.path <- tempdir()
  unzip(zip_file, exdir = shp.path)
  #' requires sf version of at least 0.8:
  sf::st_read(shp.path, layer = layer, stringsAsFactors=FALSE) %>%
    filter(!lakeCode %in% remove_IDs) %>%
    sf::st_zm() %>% st_transform(crs = 4326) %>%
    mutate(site_id = paste0('iadnr_', lakeCode), geometry = st_cast(geometry, "MULTIPOLYGON")) %>%
    mutate(site_id = str_remove(site_id, '\r\nDBE43')) %>%
    st_make_valid() %>%
    filter(!(site_id == 'iadnr_CLE17' & CONTOUR == 15),
           !(site_id == 'iadnr_STL11' & CONTOUR == 14),
           !(site_id == 'iadnr_HAN06' & CONTOUR == 2),
           !(site_id == 'iadnr_SPL30' & CONTOUR %in% 1:2),
           !(site_id == 'iadnr_WOK30' & CONTOUR ==3)) %>%
    dplyr::select(site_id, CONTOUR, geometry) %>%
    saveRDS(file = outfile)

  gd_put(out_ind, outfile)

}

slice_iadnr_contour <- function(out_ind, contour_ind){
  outfile <- as_data_file(out_ind)


  contours_sf <- sc_retrieve(contour_ind) %>% readRDS() %>%
    group_by(site_id) %>% filter(CONTOUR == min(CONTOUR)) %>% ungroup() %>%
    dplyr::select(-CONTOUR) %>%
    saveRDS(file = outfile)

  gd_put(out_ind, outfile)
}

#' get the top/shallow layer as a single polygon to represent the lake surface
slice_isro_contour <- function(out_ind, contour_ind){
  outfile <- as_data_file(out_ind)

  # need to merge/dissolve all, since these contours are filled polygons
  contours_sf <- sc_retrieve(contour_ind) %>% readRDS() %>%
    group_by(site_id) %>%
    dplyr::summarize(geometry = st_union(geometry)) %>%
    saveRDS(file = outfile)

  gd_put(out_ind, outfile)
}

fetch_mndow_lakes <- function(ind_file, layer, dummy){
  data_file <- scipiper::as_data_file(ind_file)

  zip_file <- tempfile(pattern = 'lakes.zip')

  download.file("ftp://ftp.gisdata.mn.gov/pub/gdrs/data/pub/us_mn_state_dnr/water_dnr_hydrography/shp_water_dnr_hydrography.zip",
                destfile = zip_file)

  shp.path <- tempdir()
  unzip(zip_file, exdir = shp.path)
  shp <- sf::st_read(shp.path, layer = layer, stringsAsFactors=FALSE) %>%
        filter(!is.na(dowlknum),
         !dowlknum  %in% c("00000000", "16000100"),
         wb_class %in% c("Artificial Basin", "Lake or Pond", "Mine Pit Lake", "Mine Pit Lake (NF)", "Natural Ore Mine", "Reservoir", "Riverine polygon")) %>%
  st_make_valid()

  # (16000100 is Lake Superior)

  # Duplicate DOWs (most are boundary lakes)
  dup.DOWs <- shp$dowlknum[duplicated(shp$dowlknum)] %>% unique() # 108

  # Remove duplicates with map_displa=="N" (shortcut because polys not displayed are generally older/bad)
  # + a couple tweaks that weren't covered:
  # 34028800 has an approved updated version. Remove old version
  # 86025202 Tiny pieces aren't present in parent shape and are old delineations
  shp <- shp %>% mutate(Remove = ifelse(dowlknum %in% dup.DOWs & map_displa=="N", 1, 0 )) %>%
    mutate(Remove = replace(Remove, dowlknum==34028800 & is.na(approval_d), 1)) %>%
    mutate(Remove = replace(Remove, dowlknum==86025202 & fw_id=="88888", 1)) %>%
    filter(Remove==0)


  # Fix poly for LOTW 39000201
  LOTW.shape <- shp %>% filter(dowlknum=="39000200" & outside_mn=="B")
  LOTW.39000202.shape <- shp %>% filter(dowlknum=="39000202")
  LOTW.39000201.shape <- st_difference(LOTW.shape, LOTW.39000202.shape)
  # Replace geometry in shp
  st_geometry(shp[shp$dowlknum=="39000201",]) <- st_geometry(LOTW.39000201.shape)


  # Add missing poly for Cedar Main Basin 01020901
  if(!"01020901" %in% shp$dowlknum){

    Cedar.shape <- shp %>% filter(dowlknum=="01020900")
    Cedar.01020902.shape <- shp %>% filter(dowlknum=="01020902")
    Cedar.01020903.shape <- shp %>% filter(dowlknum=="01020903")

    Cedar.01020901.shape <- st_difference(Cedar.shape, Cedar.01020902.shape)
    Cedar.01020901.shape <- st_difference(Cedar.01020901.shape, Cedar.01020903.shape)

    area_thresh <- units::set_units(800, m^2)
    Cedar.01020901.shape_dropped <- drop_crumbs(Cedar.01020901.shape, threshold = area_thresh)

    # Create 01020901 row (some of this likely not necessary since variables are dropped later)
    Row.01020901 <- shp[shp$dowlknum=="01020900",]
    st_geometry(Row.01020901) <- st_geometry(Cedar.01020901.shape_dropped)
    Row.01020901$dowlknum <- "01020901"
    Row.01020901$pw_basin_n <- "Cedar (Main)"
    Row.01020901$sub_flag <- "Y"
    Row.01020901$acres <- Row.01020901$shore_mi <- NA
    Row.01020901$unique_id <- as.character(max(as.numeric(as.character(shp$unique_id)))+1)
    row.names(Row.01020901) <- as.character(max(as.numeric(row.names(shp)))+1)

    shp <- rbind(shp, Row.01020901)
  }

  shp <- shp %>% mutate(site_id = paste0('mndow_', dowlknum)) %>% dplyr::select(site_id, geometry) %>%
  st_transform(x, crs = 4326)


  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(shp, data_file)
  gd_put(ind_file, data_file)
}




fetch_LAGOS_NE_All_Lakes_4ha <- function(ind_file){
  data_file <- scipiper::as_data_file(ind_file)
  download.file("https://portal.edirepository.org/nis/dataviewer?packageid=edi.98.3&entityid=846a50d9be7ab8262a9fd818edfcd2d0", destfile = data_file)
  gd_put(ind_file, data_file)

}

fetch_crosswalk_wqp_nhd <- function(ind_file) {

  # figure out where we will be putting the file
  data_file <- scipiper::as_data_file(ind_file)

  # download from a hard coded URL pointing to the 2017 data release; also
  # available at https://doi.org/10.5281/zenodo.595612, but the github URL
  # allows us to download just this file rather than the whole zipped repo
  url <- 'https://github.com/USGS-R/necsc-lake-modeling/blob/master/data/wqp_nhd/wqp_nhdLookup.rds?raw=true'
  download.file(url=url, destfile=data_file, mode='wb')

  # post to google drive and return an indicator file
  gd_put(ind_file, data_file)
}

fetch_micorps_sites <- function(ind_file) {

  # where to put file
  data_file <- scipiper::as_data_file(ind_file)

  # go get micorp data from gd
  googledrive::drive_download(as_id('1cWPwBnbdw7YsOHwXL9OSn8yNlmHFXKDd'), path = data_file, overwrite = TRUE)

  # post to google drive and return an indicator file
  gd_put(ind_file, data_file)

}

fetch_mo_usace_points <- function(out_ind, xlsx_ind) {
  outfile <- as_data_file(out_ind)

  mo_usace_data <- scipiper::sc_retrieve(xlsx_ind) %>%
    readxl::read_xlsx(col_types = c('numeric', 'numeric', 'text',
                                    'numeric', 'numeric'), na = 'NULL')

  # convert data to sf object and save as rds
  # rename missouri id to site id, so that rds can later be passed to `crosswalk_points_in_poly`
  # which expects input dataframe to have 'site_id' column
  mo_usace_points_sf <- st_as_sf(mo_usace_data, coords = c('x','y'), crs = 4326) %>%
    mutate(site_id = sprintf("mo_usace_%s", Station), .keep = "unused", .before = 1) %>%
    saveRDS(file = outfile)

  gd_put(out_ind, outfile)
}

fetch_navico_points <- function(out_ind, csv_ind) {
  outfile <- as_data_file(out_ind)

  navico_data <- scipiper::sc_retrieve(csv_ind) %>%
    read_csv(col_types = 'dcccdddddi', na='NULL')

  # convert data to sf object and save as rds
  # rename waterbody id to site id, so that rds can later be passed to `crosswalk_points_in_poly`
  # which expects input dataframe to have 'site_id' column
  navico_points_sf <- st_as_sf(navico_data, coords = c('CenterLong','CenterLat'), crs=4326) %>%
    mutate(site_id = sprintf("Navico_%s", MapWaterbody_ID), .keep="unused", .before=1) %>%
    saveRDS(file = outfile)

  gd_put(out_ind, outfile)
}

fetch_UNDERC_points <- function(out_ind, csv_ind){
  outfile <- as_data_file(out_ind)

  scipiper::sc_retrieve(csv_ind) %>%
    read_csv(col_types = 'dcccccdddccc') %>%
    filter(!is.na(lat) & !is.na(long)) %>%
    st_as_sf(coords = c('long','lat'), crs=4326) %>%
    mutate(site_id = sprintf("UNDERC_%s", lakeID), .keep="unused", .before=1) %>%
    saveRDS(file = outfile)

  gd_put(out_ind, outfile)
}

fetch_norfork_points <- function(out_ind, csv_ind) {
  outfile <- as_data_file(out_ind)

  norfork_data <- scipiper::sc_retrieve(csv_ind) %>%
    read_csv(col_types = 'ccddc', na = 'NULL')

  # convert data to sf object and save as rds
  # rename Site to site id, so that rds can later be passed to `crosswalk_points_in_poly`
  # which expects input dataframe to have 'site_id' column
  norfork_points_sf <- st_as_sf(norfork_data, coords = c('Long', 'Lat'), crs = 4326) %>%
    mutate(site_id = sprintf("Norfork_%s", `Unit ID`), .keep = "unused", .before = 1) %>%
    saveRDS(file = outfile)

  gd_put(out_ind, outfile)
}

fetch_Iowa_points <- function(out_ind, csv_ind){
  outfile <- as_data_file(out_ind)

  Iowa_data <- scipiper::sc_retrieve(csv_ind) %>%
    read_csv(col_types = 'icccddc')

  ia_utm14 <- Iowa_data %>% filter(Zone == "14T")
  ia_utm15 <- Iowa_data %>% filter(Zone == "15T")

  ia_utm14_to_latlong <- st_as_sf(ia_utm14, coords = c('UTM (NAD83)_E','UTM (NAD83)_N'), crs=26914) %>%
    st_transform(crs = 4326)
  ia_utm15_to_latlong <- st_as_sf(ia_utm15, coords = c('UTM (NAD83)_E','UTM (NAD83)_N'), crs=26915) %>%
    st_transform(crs = 4326)

  rbind(ia_utm14_to_latlong, ia_utm15_to_latlong) %>%
    mutate(site_id = sprintf("Iowa_%s", LakeID), .keep="unused", .before=1) %>%
    saveRDS(file = outfile)

  gd_put(out_ind, outfile)
}

fetch_univ_mo_points <- function(out_ind, csv_ind) {
  outfile <- as_data_file(out_ind)

  univ_mo_data <- scipiper::sc_retrieve(csv_ind) %>%
    read_csv(col_types = 'cicnn', na = 'NULL')

  # convert data to sf object and save as rds
  # rename Site to site id, so that rds can later be passed to `crosswalk_points_in_poly`
  # which expects input dataframe to have 'site_id' column
  univ_mo_points_sf <- st_as_sf(univ_mo_data, coords = c('Long', 'Lat'), crs = 4326) %>%
    mutate(site_id = sprintf("Missouri_%s", `Lake ID Number`), .keep = "unused", .before = 1) %>%
    saveRDS(file = outfile)

  gd_put(out_ind, outfile)
}
