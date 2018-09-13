merge_coop_dat <- function(outind, inind) {
  outfile <- as_data_file(outind)

  in_dat <- read.delim(inind, stringsAsFactors = F, header = FALSE)

  parsed_files <- gsub(': .+', '', in_dat[[1]])

  # filter criteria
  max.temp <- 40 # threshold!
  min.temp <- 0
  max.depth <- 260

  # check if file exists
  # If not, stop and throw error
  # If so, read in and bind
  all_dat <- data.frame()

  for (tmp_file in parsed_files) {

      temp_filename <- sc_retrieve(tmp_file, remake_file = '7a_temp_coop_munge_tasks.yml')
      temp_dat <- readRDS(temp_filename)
      temp_dat$source <- as_data_file(tmp_file)

      # this is a hack, should fix parsers/dependencies on parsers
      temp_dat$DateTime <- as.Date(temp_dat$DateTime)

      if ('DOW' %in% names(temp_dat)) {
        temp_dat$DOW <- as.character(temp_dat$DOW)
      }

      if ('WBIC' %in% names(temp_dat)) {
        temp_dat$WBIC <- as.character(temp_dat$WBIC)
      }

      message(paste0('Now binding ', temp_filename))
      all_dat <- bind_rows(all_dat, temp_dat)
  }

  all_dat <- filter(all_dat, !is.na(depth), !is.na(temp)) %>%
    distinct() %>% # get rid of duplicated values
    filter(depth < max.depth,
           temp > min.temp,
           temp < max.temp)

  saveRDS(object = all_dat, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
