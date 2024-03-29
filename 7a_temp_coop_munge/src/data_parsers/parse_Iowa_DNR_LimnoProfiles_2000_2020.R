
parse_Iowa_DNR_LimnoProfiles_2000_2020 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # Unzip the file with all Iowa DNR, then cleanup unzipped files
  # which aren't needed externally before leaving the function.
  # there are a lot of files in here ~4739
  unzip_dir <- tempdir()
  files_from_zip <- unzip(infile, exdir = unzip_dir)
  on.exit(unlink(unzip_dir, recursive = TRUE))

  # Generate file groups ---------------------------------
  ## This data set spans 20 yrs and is in a wide variety of formats
  ## I chose to organize by subfolder. No 2008 data was provided. 2017a was
  ## a data summary.
  files_2000_2003 <- files_from_zip[grepl('Iowa_profiles_2000-2003', files_from_zip)]
  files_2004_2007 <- files_from_zip[grepl('Iowa_profiles_2004-2007', files_from_zip)]
  files_2009 <- files_from_zip[grepl('Iowa_profiles_2009', files_from_zip)]
  files_2010 <- files_from_zip[grepl('Iowa_profiles_2010', files_from_zip)]
  files_2011_2016 <- files_from_zip[grepl('Iowa_profiles_2011-2016', files_from_zip)]
  files_2017b <- files_from_zip[grepl('Iowa_profiles_2017b', files_from_zip)]
  files_2018_2020 <- files_from_zip[grepl('Iowa_profiles_2018-2020', files_from_zip)]

  ## There are a few datasets that need further parsing
  ## this dataset has two problematic files,hence the `!is.na` at the end:
  ## 2005_L113_2005173014.xls (one record - kept it in)
  ## and 2005_L111_2005154004.xls (no data) - easiest to just remove it
  files_2004_2007 <- files_2004_2007[!(basename(files_2004_2007) %in%
                                         '2005_L111_2005154004.xls')]

  ## `2019_R3_Profiles.csv` is in an unexpected format. it was faster to just
  ## munge separately
  files_2018_2020_1 <- files_2018_2020[basename(files_2018_2020) == '2019_R3_Profiles.csv']
  files_2018_2020_2 <- files_2018_2020[!(files_2018_2020 %in% files_2018_2020_1)]

  # Clean data from 2000-2003 ------------------------------------------------
  # This function handles data where the deepest value is not the final value
  # in the profile. If there are multiple "max depths" then the final value is
  # used
  dat_2000_2003 <- files_2000_2003 %>%
    purrr::map_dfr(~ parse_2000_2009_data(.,
                                   skip_rows = 2,
                                   keep_cols = c('Depth', 'Temp'),
                                   subset_lakeid = c(6, 9))) %>%
    dplyr::mutate(time = NA, Timezone = NA)

  # Clean data from 2004-2007 ------------------------------------------------
  # skip_rows is set to zero because there is a lot of trickery in the columns
  # therefore `date` is extracted from the file name not the data set
  dat_2004_2007 <- files_2004_2007 %>%
    purrr::map_dfr(~ parse_2000_2009_data(.,
                                          skip_rows = 1,
                                          keep_cols = c('Date', 'Depth', 'Temp'),
                                          subset_lakeid = c(6, 9))) %>%
    dplyr::mutate(time = NA, Timezone = NA) %>%
    dplyr::filter(!is.na(depth))

  # Clean data from 2008 ------------------------------------------------------
  # no data from 2008 provided

  # Clean data from 2009 -----------------------------------------------------
  dat_2009 <- files_2009 %>%
    purrr::map_dfr(~ parse_2000_2009_data(.,
                                          skip_rows = 1,
                                          keep_cols = c('Date', 'Time',
                                                        'Depth', 'Temp'),
                                          subset_lakeid = c(12, 15))) %>%
    dplyr::mutate(Timezone = NA)

  # Clean data from 2010 & 2011-2016-------------------------------------------
  # these two data sets are hairy - some are true xls others are htm and there
  # is no rhyme or reason to when/why it is that way. many helper functions
  # support `parse_2010_2016_data`
  dat_2010 <- files_2010 %>%
    purrr::map_dfr(~ parse_2010_2016_data(.,
                                          keep_cols = c('Sampling_Date',
                                                        'Sampling_Time', 'Temp',
                                                        'Depth'),
                                          subset_lakeid = c(1,5),
                                          htm_datetime = F,
                                          htm_data_start = 3)) %>%
    dplyr::mutate(Timezone = NA) %>%
    dplyr::select(date = sampling_date, time = sampling_time,
                  Timezone, depth, temp, lakeid)

  dat_2011_2016 <- files_2011_2016 %>%
    purrr::map_dfr(~ parse_2010_2016_data(.,
                                          keep_cols = c('Sampling_Date_Time',
                                                        'Temp', 'Depth'),
                                          subset_lakeid = c(1,5),
                                          htm_datetime = T,
                                          htm_data_start = 3,
                                          htm_verbose = F)) %>%
    dplyr::mutate(date = as.Date(sampling_date_time),
                  time = format(sampling_date_time, '%H:%M'),
                  Timezone = NA) %>%
    dplyr::select(date, time, Timezone, depth, temp, lakeid)

  # Clean data from 2017a and 2017b----------------------------------------------
  #' 2017a data does not have date or time
  #' 2017b data does have date and time. Lake name needs to be parsed out of file.

  dat_2017b <- files_2017b %>%
    purrr::map_dfr(~ parse_2017_2020_data(.,
                                          keep_cols = c('DATE', 'TIME', 'SITE',
                                                        'Temp...F.', 'Depth..m.'),
                                          lakeid_col = 3,
                                          temp_as_f = T,
                                          use_readr = F)) %>%
    dplyr::mutate(Timezone = NA) %>%
    dplyr::rename(depth = depth..m.,
           temp = temp...f.) %>%
    dplyr::select(date, time, Timezone, depth, temp, lakeid)

  # Clean data from 2018-2020 ---------------------------------------------------
  dat_2018_2020_1 <- files_2018_2020_1 %>%
    purrr::map_dfr(~ parse_2017_2020_data(.,
                                          keep_cols = c('DATE', 'DATE.1', 'TIME',
                                                        'Depth..m.', 'Temperature..Deg.C.'),
                                          lakeid_col = 1,
                                          temp_as_f = F,
                                          use_readr = F)) %>%
    dplyr::mutate(Timezone = NA) %>%
    dplyr::rename(depth = depth..m.,
                  temp = temperature..deg.c.) %>%
    dplyr::select('date', 'time', 'Timezone', 'depth', 'temp', 'lakeid')

  dat_2018_2020_2 <- files_2018_2020_2 %>%
    purrr::map_dfr(~ parse_2017_2020_data(.,
                                          keep_cols = c('Lake', 'Date', 'Time',
                                                        'Depth (m)', 'Temperature (Deg C)'),
                                          lakeid_col = 1,
                                          temp_as_f = F,
                                          use_readr = T)) %>%
    dplyr::mutate(Timezone = NA) %>%
    dplyr::rename(depth = `depth (m)`,
           temp = `temperature (deg c)`) %>%
    dplyr::select('date', 'time', 'Timezone', 'depth', 'temp', 'lakeid')

  # clean data -------------------
  data_clean <- dplyr::bind_rows(dat_2000_2003,
                          dat_2004_2007,
                          dat_2009,
                          dat_2010,
                          dat_2011_2016,
                          dat_2017b,
                          dat_2018_2020_1,
                          dat_2018_2020_2) %>%
    dplyr::mutate(Iowa_ID = paste('Iowa_', lakeid, sep = '')) %>% # for matching the xwalk
    dplyr::select(DateTime = date, time, Timezone, depth, temp, Iowa_ID) # remove holdover columns from 2017-2020 data

  # out -------------------
  saveRDS(object = data_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}



# Parsers by subfolder -----------------------------------------

#' Parse Iowa DNR data from 2000-2009
#'
#' This data does not contain lake id, date, or time in the data file. Instead,
#' these values are extracted from the name of the file. This parser also
#' accounts for profiles that include data from the descent _and_ the ascent
#' of the data logger. According to cooperators, the ascent data should
#' be removed.
#'
#' @param file_path chr, full file path
#' @param skip_rows int, number of rows to skip when reading in data. Be sure to . Start the
#' @param keep_cols chr, vector of column names to keep.
#' @param subset_lakeid num, vector for start/stop values passed to `base::substr` in `extract_lakeid`
#' Column name spelling should match. Case does not matter because
#' all values to be changed `tolower` within the function.
#'
parse_2000_2009_data <- function(file_path, skip_rows = 0, keep_cols,
                                 subset_lakeid = c(6, 9)) {

  dat <- suppressMessages(readxl::read_xls(file_path, skip = skip_rows, col_names = F))
  names(dat) <- suppressMessages(readxl::read_xls(file_path, n_max = 1, col_names = F)) %>%
    unlist %>%
    .[c(1:ncol(dat))] %>%  # some data sets have an empty last row
    tolower

  dat <- dat %>%
    dplyr::select(all_of(tolower(keep_cols))) %>%
    dplyr::mutate(depth = as.numeric(depth), temp = as.numeric(temp))

  # add LakeID and date columns from file name if necessary
  dat$lakeid <- extract_lakeid(file_path, start = subset_lakeid[1],
                               stop = subset_lakeid[2])

  # wrangle date and time
  df_names <- names(dat)

  ## No date field present
  if(!any(c('date', 'sampling_date_time') %in% df_names)) {
    dat$date <- extract_date(file_path, subset_yr = c(1,4),
                             subset_jday = c(15, 17))
  }

  if('time' %in% names(dat)) {
    # note some profiles contain time in HMS and values are reported every 1 sec
    # currently all data is left in (e.g. no aggregation), so you may see data
    # at multiple time steps
    dat$time <- format(dat$time, "%H:%M")
  }

  # final date normalization to ensure `as.Date` (some are coming in as `POSIXct`)
  dat$date <- as.Date(dat$date)

  # Check to see if data.frame has data
  dat_rows <- nrow(dat)

  if(dat_rows > 1) {
    # check max depth and trim data if necessary
    if(check_final_depth(dat$depth)) {
      depth_loc <- which(names(dat) == 'depth') # id position of the depth column
      dat <- trim_to_max_depth(dat, depth_col = depth_loc)

      # return(dat)
    }
  } else {
    message(paste('File ', basename(file_path),
                ' appears to be empty or has problematic data. Data only has ',
                dat_rows, ' row(s) of data', sep = ''))
  }

  return(dat)
}

#' Parse Iowa DNR data from 2010-2016
#'
#' Data parser for data from 2010-2016. These files are in a variety of
#' mismatched formats. this parser can differentiate between `xls` and
#' `htm` files that are labeled as `xls`
#'
#' @param file_path path, full file path
#' @param keep_cols chr, vector of columns names that should be preserved
#' @param subset_lakeid num, vector for start/stop values passed to `base::substr` in `extract_lakeid`
#' Column name spelling should match. Case does not matter because
#' all values to be changed `tolower` within the function.
#' @param htm_datetime logical, `TRUE` if the file is expected to have
#' a `datetime` field. This is not always the case in the 2016 data. Argument
#' used if data is `htm` format.
#' @param htm_data_start num, on what row does the data start? Argument used if
#' data is `htm` format.
#' @param htm_verbose logical, if `TRUE` a message that identifies an HTM file
#' is returned. Argument used if data is `htm` format.
#'
parse_2010_2016_data <- function(file_path, keep_cols, subset_lakeid, htm_datetime = F,
                                 htm_data_start = 2, htm_verbose = F) {
  tc_out <- tryCatch(
    { read_iowa_xls_tbl(file_path, keep_cols, data_start = 3) }, # current thinking is that all true xls start on row 3
    error = function(cond) {
      if(htm_verbose) {print(paste(basename(x), ' is in HTM format', sep = ''))}
      read_iowa_htm_tbl(file_path, keep_cols, htm_datetime, htm_data_start)
    }
  )

  # add LakeID from file name if necessary
  tc_out$lakeid <- extract_lakeid(file_path,
                                  start = subset_lakeid[1], stop = subset_lakeid[2])

  return(tc_out)
}

#' Parse Iowas data from 2017-2020
#'
#' @param file_path chr, full file path
#' @param keep_cols chr, vector of column names to keep.
#' @param lakeid_col num, Column index for the location of the Lake ID field. The column that is used to ID the Iowa lake moves around and changes names from file to file.
#' @param temp_as_f logical, should temp values be converted from F to C? Note, this does convert the vector but the field name will not change.
#' @param use_readr logical, not all files are read well by `readr::read_csv`. If `use_readr = F` then `read.csv` is used.
#' Column name spelling should match. Case does not matter because
#' all values to be changed `tolower` within the function.
parse_2017_2020_data <- function(file_path, keep_cols, lakeid_col,
                                 temp_as_f = F, use_readr = T){

  if(grepl('*xlsx', file_path)) {
    dat <- readxl::read_xlsx(file_path)
  } else if (use_readr){
    dat <- readr::read_csv(file_path)
  } else {
    dat <- read.csv(file_path, fileEncoding = 'latin1')
  }

  # select key columns, normalize
  names(dat) <- tolower(names(dat))
  dat <- dat %>% dplyr::select(all_of(tolower(keep_cols)))

  # validate fields
  if(!any(names(dat) %in% c('lake', 'site'))) {
    # this is specifically to accommodate one file: "2019_R3_Profiles.csv"
    # 2017 data is generally called 'site' and 2018-2020 is 'lake' for the id field
    names(dat)[lakeid_col] <- 'lake'
    idx_date <- grepl('*date*', names(dat)) %>% which(. %in% "TRUE")
    names(dat)[idx_date] <- 'date'
  }

  # add numeric lakeid - this comes in a variety of flavors between 2017-2020,
  # just extracting the numbers
  dat$lakeid <- sapply(dat[ , lakeid_col], function(x) {
    as.numeric(gsub(".*?([0-9]+).*", "\\1", x))
  })

  # handle date
  if(any(class(dat$date) == 'character')) {
    dat$date <- lubridate::mdy(dat$date)
  }

  # ensure all dates are `as.Date` (some come in as `POSIXct`)
  dat$date <- as.Date(dat$date)

  # handle time - check for datetimes and AM/PM designation
  if(any(sapply(dat$time, function(x) class(x) == "POSIXct"))) {

    dat$time <- strftime(dat$time, format = "%H:%M") %>% as.character()

  } else if(any(sapply(dat$time, function(x) nchar(x) > 9))) {

    # chr fields that include AM/PM are 10 or 11 characters long; therefore,
    # the parsing needs to be slightly different (e.g., extracting "9:55" out
    # of "9:55:05 AM" vs "11:25" out of "11:25:35 AM")
    dat$time <- ifelse(nchar(dat$time) == 11,
                       substr(dat$time, 1, 5),
                       substr(dat$time, 1, 4))

  } else {

    # 2018-2020 data has a little extra whitespace and often includes the seconds
    dat$time <- format(dat$time, '%H:%M') %>%
      as.character(.) %>%
      substr(., 1, 5)

  }

  # convert temp if necessary. Temp field moves around so it needs
  # to be identified first
  if(temp_as_f) {
    idx <- grepl('*temp*', names(dat)) %>% which(. %in% "TRUE")
    dat[ , idx] <- fahrenheit_to_celsius(dat[ , idx])
  }

  return(dat)
}




# Helper functions used in parsers -----------------------------------------

#' Read in 2010-2016 data from HTM disguised as XLS
#'
#' @param file_path chr, full file path
#' @param keep_cols chr, vector of column names that should be preserved
#' @param data_start num, row number where the data starts
#'
read_iowa_xls_tbl <- function(file_path, keep_cols, data_start){

  sk <- data_start - 1

  # read data
  out <- suppressMessages(readxl::read_xls(file_path, skip = sk, col_names = F))

  # add names
  names(out) <- suppressMessages(
    readxl::read_xls(file_path, n_max = 1, col_names = F) %>%
      unlist %>%
      tolower
  )

  # select columns of interest
  out <- out %>% dplyr::select(all_of(tolower(keep_cols)))

  return(out)
}


#' Read in 2010-2016 data from HTM disguised as XLS
#'
#'
#' @param file_path chr, full file path
#' @param keep_cols chr, vector of column names that should be preserved
#' @param datetime logical, is there a datetime column?
#' @param data_start num, row number where the data starts
#'
read_iowa_htm_tbl <- function(file_path, keep_cols, datetime = F, data_start = 3){

  # read data
  out <- rvest::html_table(rvest::read_html(file_path, col_names = F)) %>%
    .[[1]] %>%
    na_if("") %>% #%>% # scraping HTML often results in `""` in cells at the end
    remove_na_rows

  # parse and reformat column names
  names(out) <- out[1, ] %>%
    unlist %>%
    tolower # assign col names

  # select columns of interest
  out <- out %>% dplyr::select(all_of(tolower(keep_cols)))

  # handle date/datetime
  if(datetime) {
    out <- out[c(data_start:nrow(out)), ]
    out[c(2:ncol(out))] <- lapply(out[c(2:ncol(out))], as.numeric)

    # most of the 2011-2016 data contains datetime in `Sampling_Date_Time`
    # but the time component doesn't always come along for the ride
    out[, 1] <- tryCatch(lubridate::mdy_hm(unlist(out[ , 1])),
                         warning = function(w) {lubridate::mdy(unlist(out[ , 1]))})
  } else {
    out <- out[c(data_start:nrow(out)), ]
    out[c(3:ncol(out))] <- lapply(out[c(3:ncol(out))], as.numeric)

    # 2010 data includes a date field and a time field
    # The final `sapply` removes a few lagging colons (e.g., "9:58:")
    out[ , 1] <- lubridate::mdy(unlist(out[ , 1]))
    out[ , 2] <- str_sub(unlist(out[ , 2]), start = 1, end = 5)
    out[ , 2] <- sapply(unlist(out[ ,2]), function(x) {
      ifelse(stringr::str_ends(x, ":"),
             stringr::str_sub(x, 1, 4), x)
    })
  }

  return(out)

}

#' Remove rows that are blank (all cells == NA)
#'
#' @param x data.frame
#'
remove_na_rows <- function(x) {
  idx <- apply(x, 1, function(x) all(is.na(x)))
  x[!(apply(x, 1, function(df) all(is.na(df)))), ]
}

#' Check the maximum depth of the lake profile
#'
#' The `explain` file for 2000-2003 Iowa DNR data (`Iowa_profiles_2000-2003.docx`)
#' notes that some ascent data is included in the datasets, but shouldn't be
#' used. This function IDs the lowest depth by returning `TRUE` if the maximum
#' depth is not the final value in the profile
#'
#' @param depth_vector num, a vector of depth values
#'
check_final_depth <- function(depth_vector) {
  depth_max <- max(depth_vector, na.rm = T)
  depth_max_location <- which(depth_vector == depth_max) %>% max

  depth_max_location < length(depth_vector)
}

#' Trim the lake profile to the maximum depth
#'
#' The `explain` file for 2000-2003 Iowa DNR data (`Iowa_profiles_2000-2003.docx`)
#' notes that some datasets contain data from the descent _and_ the ascent of the data
#' logger. The ascent data should be trimmed.
#'
#' @param df data.frame that includes the depth
#' @param depth_col int, index for depth column. Defaults to the first column (`depth_col = 1`).
#'
trim_to_max_depth <- function(df, depth_col = 1) {
  # identify the row index of the max depth
  # choose the maximum index if there are multiple
  index_max_depth <- which(df[, depth_col] == max(df[, depth_col], na.rm = T)) %>%
    max

  df_trimmed <- df[c(1:index_max_depth), ]

  return(df_trimmed)
}

#' Parse important information from file name
#'
#' Not all files include a `Date` column. This function extracts the sample
#' date from filename (formatted as a julian day). This function returns
#' the sample date in `Date` format.
#'
#' @param file_path chr, full file path
#' @param subset_yr int, provide a start and stop values as a vector. Used to select sample year and similar to `start`/`stop` in `base::substr()`.
#' @param subset_jday int, provide a start and stop values as a vector. Used to select sample year and similar to `start`/`stop` in `base::substr()`.
#'
extract_date <- function(file_path, subset_yr, subset_jday) {
  # get year and julian day from file name
  yr <- substr(basename(file_path), subset_yr[1], subset_yr[2])
  julian_day <- as.numeric(substr(basename(file_path),
                                  subset_jday[1], subset_jday[2]))

  # build origin
  origin_date <- as.Date(paste(yr, '-01-01', sep = ''))

  #determine sample date
  origin_date + julian_day
}

#' Extract Lake ID from file name
#'
#' Not all files include a field that identifies the sampled lake. This
#' function extracts the Lake ID from the Iowa DNR data set. This value is
#' typically in the format `L128` or `L_128`. This function returns the
#' lake id as a numeric value for use with the LakeID field in
#' `1_crosswalk_fetch/in/Iowa_Lake_Locations.csv`
#'
#' @param file_path chr, full file path
#' @param ... further arguments passed to `substr` (`start` and `stop`)
#'
extract_lakeid <- function(file_path, ...) {
  clean_name <- substr(basename(file_path), ...) %>%
    gsub(".*?([0-9]+).*", "\\1", .) %>%
    as.numeric

  return(clean_name)
}
