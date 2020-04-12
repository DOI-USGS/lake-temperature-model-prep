

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
  googledrive::drive_download(as_id('1cWPwBnbdw7YsOHwXL9OSn8yNlmHFXKDd'), path = data_file)

  # post to google drive and return an indicator file
  gd_put(ind_file, data_file)

}

#' use `dummy` to trigger rebuilds. I am using the date, as a light reminder of when it was changed
fetch_wqp_lake_sites <- function(ind_file, characteristicName, bBox, dummy){

  message('warning, avoiding geojson output due to issue with services 2020-01-19
          hacking resultCount as 200')

  # when switching back in the future, seems lat and lon were the names for LatitudeMeasure & LongitudeMeasure
  lake_sites_sf <- whatWQPsites(siteType = "Lake, Reservoir, Impoundment", characteristicName = characteristicName,
                                bBox = bBox) %>%
    mutate(resultCount = 200) %>%
    dplyr::select(site_id = MonitoringLocationIdentifier, resultCount, LatitudeMeasure, LongitudeMeasure) %>%
    st_as_sf(coords = c("LongitudeMeasure", "LatitudeMeasure"), crs = 4326)

  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(lake_sites_sf, data_file)
  gd_put(ind_file, data_file)
}
