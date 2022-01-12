# Function to pull down a zipped shapeful from a url and read in as an `sf` object
# Originally developed to use for getting Census counties, so assumes that the shapefiles
# have the following fields: `STATEFP` (fips code) and `NAME` (county name)
# Also used in `8_viz/src/plot_lake_obs_map.R`
fetch_zip_url_sf <- function(ind_file, zip_url, layer_name){

  destination = tempfile(pattern = layer_name, fileext='.zip')
  file <- httr::GET(zip_url, httr::write_disk(destination, overwrite=T), httr::progress())
  shp_path <- tempdir()
  unzip(destination, exdir = shp_path)

  us_counties_sf <- sf::st_read(shp_path, layer=layer_name) %>%
    sf::st_transform(crs = 4326) %>%
    mutate(state = dataRetrieval::stateCdLookup(STATEFP)) %>%
    dplyr::select(state, county = NAME)

  # write, post, and promise the file is posted
  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(us_counties_sf, data_file)
  gd_put(ind_file, data_file)

}
