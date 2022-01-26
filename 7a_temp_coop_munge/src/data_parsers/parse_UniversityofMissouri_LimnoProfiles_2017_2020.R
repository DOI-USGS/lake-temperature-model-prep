#' Parse the 106 files within the `UniversityofMissouri_LimnoProfiles_2017-2020.zip` file

parse_UniversityofMissouri_LimnoProfiles_2017_2020 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # Unzip the file with all University of MO data, then cleanup unzipped files
  # which aren't needed externally before leaving the function.
  unzip_dir <- tempdir()
  files_from_zip <- unzip(infile, exdir = unzip_dir)
  on.exit(unlink(unzip_dir, recursive = TRUE))

  # Set grouping variables and define relevant column names for each data set -------------

  # Files fall into roughly 5 categories, as defined below.
  # A few notes on the files in `files_2017_hw`:
  # (1) `~/092_08-02-2017 HW.csv` is duplicated as `~/092_08_02_2017 HW.csv`.
  # (2) The file format for both files in (1) doesn't match any other file in the 2017 HW files
  # Issues (1) and (2) are resolved by removing them both from the `files_2017_hw`
  # and then adding one (`092_08_02_2017 HW.csv`) into it's own vector
  files_2017 <- files_from_zip[grepl("_2017_.*cleaned", files_from_zip)]
  files_2018_2020 <- files_from_zip[grepl('_2018.*cleaned|_2019|_2020',files_from_zip)]
  files_2017_hw <- files_from_zip[grepl('_2017_', files_from_zip)] %>%
    .[grepl('HW', .)] %>%
    .[!grepl('092_08_02_2017|092_08-02-2017', .)]
  files_2017_092_hw <-  files_from_zip[grepl('092_08_02_2017 HW.csv', files_from_zip)]
  files_2018_hw <- files_from_zip[grepl('_2018.*HW', files_from_zip)]

  # Sanity check for data consistency after grouping
  # subtracting 1 from `length(files_from_zip)` because of removal of
  # duplicate file `~/092_08-02-2017 HW.csv`
  groupings <- c(files_2017_hw, files_2017_092_hw, files_2018_hw,
                 files_2017, files_2018_2020)
  if ((length(files_from_zip) - 1) != lapply(groupings, length) %>% unlist %>% sum) {
    stop('Number of files in subfolders does not match grouping')
  }

  # column selection for each data grouping
  # column names will be forced `tolower` in the parsers because
  # case is inconsistent file-to-file
  keep_cols_2017 <- c('Date', 'Time', 'Dep.M', 'X.C')
  keep_cols_2018_2020 <- c('Date..MM.DD.YYYY.', 'Time..HH.MM.SS.',
                           'Depth.m', 'Temp..C')

  # Clean data from 2017 with consistent formatting --------------------
  dat_2017 <- files_2017 %>%
    purrr::map_dfr(~ read_subset_consistent_data(., keep_cols = keep_cols_2017)) %>%
    dplyr::mutate(
      DateTime = lubridate::mdy(date),
      time = strftime(time, "%H:%M"),
      Timezone = NA,
      depth = dep.m,
      temp = x.c,
      Missouri_ID = lake_id,
      .keep = 'none'
    )

  # Clean data from 2018-2020 with consistent formatting --------------------
  dat_2018_2020 <- files_2018_2020 %>%
    purrr::map_dfr(~ read_subset_consistent_data(., keep_cols = keep_cols_2018_2020)) %>%
    dplyr::mutate(
      DateTime = lubridate::mdy(date..mm.dd.yyyy.),
      time = strftime(time..hh.mm.ss., "%H:%M"),
      Timezone = NA,
      depth = depth.m,
      temp = temp..c,
      Missouri_ID = lake_id,
      .keep = 'none'
    )

  # Clean 2017 & 2018 data from hand-transcribed field sheets ----------------------

  dat_2017_hw <- files_2017_hw %>%
    purrr::map_dfr(~ read_subset_hw_files(.))

  dat_2017_hw_092 <- read_subset_hw_files(files_2017_092_hw,
                                          skip_rows = 15,
                                          skip_cols = 2,
                                          depth_position = 16,
                                          temp_position = 3)

  dat_2018_hw <- files_2018_hw %>%
    purrr::map_dfr(~ read_subset_hw_files(., skip_rows = 0))

  all_hw_files <- dplyr::bind_rows(dat_2017_hw, dat_2017_hw_092, dat_2018_hw) %>%
    dplyr::mutate(
      DateTime = date,
      time = time,
      Timezone = NA,
      depth = depth,
      temp = temp,
      Missouri_ID = lake_id,
      .keep = 'none'
    )

  # combine all data sets together ------------------------------------------
  data_clean <- dplyr::bind_rows(
    dat_2017,
    dat_2018_2020,
    all_hw_files
  )

  saveRDS(object = data_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}

# Read data functions -----------------------------------

#' Read and clean files from 2019-2020 UMO data
#'
#' @param full_path chr, full file path
#' @param keep_cols chr, vector of column names to keep
#'
read_subset_consistent_data <- function(full_path, keep_cols) {
  dat <- suppressMessages(readr::read_csv(full_path, name_repair = 'universal',
                                          locale = locale(encoding = "latin1")))

  # convert field names to lowercase because field names are consistent but
  # use of capitalization is not
  names(dat) <- tolower(names(dat))
  dat <- dat %>% dplyr::select(all_of(tolower(keep_cols)))

  # extract lake id from file name because
  # use of `Site.Name` field is inconsistent
  dat$lake_id <- paste('Missouri_', extract_umo_lake_id(full_path), sep = '')

  return(dat)
}

#' Read and clean hand-transcribed files from 2017 and 2018 UMO data
#'
#' this function handles all files tagged `*_HW*`except `092_08_02_2017 HW.csv`.
#'
#' @param full_path chr, full file path
#' @param skip_cols num, number of columns to skip before starting to read data.frame
#' @param skip_rows num, number of rows to skip before starting to read data.frame
#' @param depth_position num, column number associated with `depth`
#' @param temp_position num, column number associated with `temperature`
#'
read_subset_hw_files <- function(full_path, skip_cols = 0, skip_rows = 2,
                                 depth_position = 1, temp_position = 2) {
  rstart <- skip_rows + 1
  cstart <- skip_cols + 1

  dat <- suppressMessages(read_csv(full_path, col_names = F,
                                   locale = locale(encoding = "latin1"))) %>%
    remove_empty_columns()

  col_nms <- as.character(dat[rstart, ]) %>%
    tolower %>%
    .[cstart:length(.)]

  dat <- dat %>% .[(rstart + 1):nrow(.), cstart:ncol(.)]
  names(dat) <- col_nms

  dat <- dat %>%
    dplyr::mutate(
      depth = as.numeric(.[[(depth_position - skip_cols)]]),
      temp = as.numeric(.[[(temp_position - skip_cols)]]),
      date = extract_umo_date(full_path),
      time = NA, # skipping time for now, in the manual files the time moves around
      lake_id = paste('Missouri_', extract_umo_lake_id(full_path), sep = '')
    ) %>%
    dplyr::select(depth, temp, date, time, lake_id) # name of temp and depth vary

  return(dat)
}

# Additional helper functions ----------------------------------

#' Extract the UMO Lake ID from the file name
#'
#' The lake ID is not always included in the data. This function creates
#' a lake ID based on the info provided in the file name.
#'
#' @param file_path chr, full file path
#'
extract_umo_lake_id <- function(full_path) {
  as.numeric(substr(basename(full_path), 1, 3))
}

#' Extract the sample date from the UMO file name
#'
#' Sample data is not always included in the data. This function creates
#' a sample date based on the date provided in the file name.
#'
#' @param file_path chr, full file path
#'
extract_umo_date <- function(full_path) {
  date_raw <- substr(basename(full_path), 5, 14)
  lubridate::parse_date_time(date_raw, orders = c('ymd', 'mdy')) # multiple formats appear
}

#' Remove empty columns
#'
#' A few of the files import with blank columns. This function removes the
#' empty columns.
#'
#' @param x a data.frame
#'
remove_empty_columns <- function(x){
  empty_columns <- sapply(x, function(x) all(is.na(x) | x == ""))
  x <- x[, !empty_columns]
}
