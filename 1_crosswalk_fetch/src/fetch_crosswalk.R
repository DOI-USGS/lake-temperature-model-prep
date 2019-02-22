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

fetch_crosswalk_wbic_nhd <- function(ind_file) {

  # figure out where we will be putting the file
  data_file <- scipiper::as_data_file(ind_file)

  # download from a github URL that points to necsc-lake-modeling repo
  url <- 'https://github.com/USGS-R/necsc-lake-modeling/blob/master/data/NHD_state_crosswalk/nhd2wbic.RData?raw=true'
  download.file(url=url, destfile=data_file, mode = 'wb')

  # post to google drive and return an indicator file
  gd_put(ind_file, data_file)
}

fetch_crosswalk_dow_nhd <- function(ind_file) {

  # figure out where we will be putting the file
  data_file <- scipiper::as_data_file(ind_file)

  # download from a github URL that points to necsc-lake-modeling repo
  url <- 'https://github.com/USGS-R/necsc-lake-modeling/blob/master/data/NHD_state_crosswalk/nhd2dowlknum.RData?raw=true'
  download.file(url=url, destfile=data_file, mode = 'wb')

  # post to google drive and return an indicator file
  gd_put(ind_file, data_file)
}
