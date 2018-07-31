#' Download the curated shapefile from Winslow et al. 2017 data release,
#' http://dx.doi.org/10.5066/F7DV1H10, "Spatial data" item.
#'
#' Here we return a single indicator file to represent the entirety of
fetch_crosswalk_shapefile <- function(ind_file) {

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
    dest <- file.path(out_dir, sprintf('%s.%s', filename_body, filename_ext))
    download.file(url=shapefile$downloadUri, destfile=dest, mode='wb')
  }

  # post to google drive and return an indicator file
  gd_put(ind_file, data_file)
}
