#' Download the curated shapefile from Winslow et al. 2017 data release,
#' http://dx.doi.org/10.5066/F7DV1H10, "Spatial data" item.
#'
#' Here we return a single indicator file to represent the entirety of
fetch_winslow_shapefile <- function(ind_file) {

  # figure out where we will be putting the files
  data_file <- scipiper::as_data_file(ind_file)
  out_dir <- dirname(data_file)
  filename_body <- tools::file_path_sans_ext(basename(data_file))
  filename_ext <- tools::file_ext(data_file)

  # here's the ScienceBase item we're downloading from:
  item <- sbtools::item_get(sb_id='57d97341e4b090824ffb0e6f')

  # download the file.
  if(filename_ext == 'xml') {
    xml_file <- sbtools::item_list_files(sb_id=item) %>%
      filter(fname == 'lakes.xml')
    sbtools::item_file_download(sb_id=item, names=xml_file$fname, destinations=data_file, overwrite_file=TRUE)
  } else {
    # item_list_files and item_file_download are not currently recognizing any
    # of the shapefile extensions, so we instead get each file's URI by digging
    # into the item data
    shapefiles <- item$facets[[1]]$files
    shapefile <- shapefiles[[which(vapply(shapefiles, function(shapefile) { tools::file_ext(shapefile$name) == filename_ext }, FUN.VALUE=TRUE))]]
    download.file(url=shapefile$downloadUri, destfile=data_file, mode='wb')
  }

  # post to google drive and return an indicator file
  gd_put(ind_file, data_file)
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
         wb_class %in% c("Artificial Basin", "Lake or Pond", "Mine Pit Lake", "Mine Pit Lake (NF)", "Natural Ore Mine", "Reservoir", "Riverine polygon"))

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
fetch_wqp_lake_sites <- function(ind_file, characteristicName, dummy){
  lake_sites_sf <- whatWQPdata(siteType = "Lake, Reservoir, Impoundment", characteristicName = characteristicName) %>%
    dplyr::select(site_id = MonitoringLocationIdentifier, resultCount, LatitudeMeasure = lat, LongitudeMeasure = lon) %>%
    st_as_sf(coords = c("LongitudeMeasure", "LatitudeMeasure"), crs = 4326)

  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(lake_sites_sf, data_file)
  gd_put(ind_file, data_file)
}

