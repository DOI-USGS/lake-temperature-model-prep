

munge_nlcd_buffers <- function(out_ind, lake_buffer_ind, nlcd_zip_ind, nlcd_classes_ind){
  lake_buffers <- sc_retrieve(lake_buffer_ind) %>% readRDS
  nlcd_classes <- sc_retrieve(nlcd_classes_ind) %>% readRDS
  nlcd_zip <- sc_retrieve(nlcd_zip_ind)
  # now unzip and read in...
  raster_path <- tempdir()

  zip_path <- file.path(getwd(), nlcd_zip)

  decompress_file(file = zip_path, directory = raster_path)

  zip_folder_name <- tools::file_path_sans_ext(basename(nlcd_zip))
  nlcd_file <- file.path(raster_path, zip_folder_name, paste0(zip_folder_name, '.img'))

  NLCD <- raster(nlcd_file)

  # modified old code from
  # https://github.com/USGS-R/necsc-lake-modeling/blob/d37377ea422b9be324e8bd203fc6eecc36966401/scripts/lake_sheltering_nlcd.R#L3

  shelter_lakes <- st_transform(lake_buffers, crs = st_crs(proj4string(NLCD)))
  ids <- shelter_lakes$site_id
  # loop through lakes, crop and mask, remove NAs (from mask), calc class percentages, bind:

  data.out <- matrix(data = NA, nrow = length(ids), ncol = nrow(nlcd_classes))
  for (i in 1:length(ids)){
    lake <- shelter_lakes[i, ] %>% as('Spatial')

    buffer.data <- raster::crop(NLCD, lake) %>%
      mask(lake) %>%
      freq() %>% data.frame %>% filter(!is.na(value))

    tot.px <- sum(buffer.data$count)

    data.out[i, ] <- mutate(buffer.data, perc = count/tot.px*100) %>%
      rename(class=value) %>%
      right_join(nlcd_classes, by="class") %>% arrange(class) %>% mutate(perc=ifelse(is.na(perc), 0, perc)) %>%
      dplyr::select(class, perc) %>%
      tidyr::spread(key='class','perc') %>% as.numeric

    if (i %% 100 == 0)
      cat(i,' of ', length(ids))
    cat('.')
  }
  shelter_out <- data.frame(data.out) %>% setNames(paste0('nlcd_class.',nlcd_classes$class)) %>%
    cbind(data.frame(id=ids))

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(shelter_out, data_file)
  gd_put(out_ind, data_file)
}


# Modified from https://stackoverflow.com/questions/42740206/r-possible-truncation-of-4gb-file
# to use 7zip
decompress_file <- function(directory, file, .file_cache = FALSE) {

  if (.file_cache == TRUE) {
    print("decompression skipped")
  } else {

    # Run decompression
    decompression <-
      system2("7z",
              args = c("x",
                       file,
                       paste0('-o',directory)),
              stdout = TRUE)

    # uncomment to delete archive once decompressed
    # file.remove(file)

    # Test for success criteria
    # change the search depending on
    # your implementation
    if (grepl("Warning message", tail(decompression, 1))) {
      print(decompression)
    }
  }
}
