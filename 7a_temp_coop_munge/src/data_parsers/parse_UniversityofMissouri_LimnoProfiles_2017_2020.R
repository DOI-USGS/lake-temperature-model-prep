#' Parse the 106 files within the `UniversityofMissouri_LimnoProfiles_2017-2020.zip` file

parse_UniversityofMissouri_LimnoProfiles_2017_2020 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # Unzip the file with all University of MO data, then cleanup unzipped files
  # which aren't needed externally before leaving the function.
  unzip_dir <- tempdir()
  files_from_zip <- unzip(infile, exdir = unzip_dir)
  on.exit(unlink(unzip_dir, recursive = TRUE))

  # Files fall into roughly 5 categories, as defined below.
  # A few notes on the files in `files_2017_hw`:
  # (1) `~/092_08-02-2017 HW.csv` is duplicated as `~/092_08_02_2017 HW.csv`.
  # (2) The file format for both files in (1) doesn't match any other file in the 2017 HW files
  # Issues (1) and (2) are resolved by removing them both from the `files_2017_hw`
  # and then adding one (`092_08_02_2017 HW.csv`) into it's own vector
  files_tbl <- tibble(filepath = files_from_zip) %>%
    mutate(func_name = case_when(
      str_detect(filepath, '_2017_.*cleaned') ~
        'read_files_2017',
      str_detect(filepath, '_2018.*cleaned|_2019|_2020') ~
        'read_files_2018_2020',
      str_detect(filepath, '_2017_') & str_detect(filepath, 'HW') & !str_detect(filepath, '092_08_02_2017|092_08-02-2017') ~
        'read_files_2017_hw',
      str_detect(filepath, '092_08_02_2017 HW.csv') ~
        'read_files_2017_092_hw',
      str_detect(filepath, '_2018.*HW') ~
        'read_files_2018_hw'
      # if a file doesn't below to a group, this column will be NA:
    )) %>% filter(!is.na(func_name))

  # Sanity check for data consistency after grouping
  # subtracting 1 from `length(files_from_zip)` because of removal of
  # duplicate file `~/092_08-02-2017 HW.csv`
  if ((length(files_from_zip) - 1) != nrow(files_tbl)) {
    stop('Number of files in subfolders does not match grouping')
  }


  read_files_2017 <- function(df){
    read_subset_consistent_data(df, keep_cols = c('Date', 'Time', 'Dep.M', 'X.C'),
                                temp_col = 'x.c')

  }
  read_files_2018_2020 <- function(df){
    read_subset_consistent_data(df, keep_cols = c('Date..MM.DD.YYYY.', 'Time..HH.MM.SS.',
                                      'Depth.m', 'Temp..C'),
                                date_col = 'date..mm.dd.yyyy.', time_col = 'time..hh.mm.ss.',
                                depth_col = 'depth.m', temp_col = 'temp..c')
  }

  read_files_2017_hw <- function(df){
    read_subset_hw_files(df)
  }

  read_files_2017_092_hw <- function(df){
    read_subset_hw_files(df, skip_rows = 15,
                         skip_cols = 2,
                         depth_position = 16,
                         temp_position = 3)
  }

  read_files_2018_hw <- function(df){
    read_subset_hw_files(df, skip_rows = 0)
  }

  # this goes through all rows of the data.frame and dispatches to the functions
  # we assigned above. Since all functions return the same type of data.frame,
  # they can be combined with `bind_rows`
  data_clean <- purrr::pmap(files_tbl, function(filepath, func_name){
    exec(func_name, filepath)
  }) %>% bind_rows


  saveRDS(object = data_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}

# Read data functions -----------------------------------

#' Read and clean files from 2019-2020 UMO data
#'
#' @param full_path chr, full file path
#' @param keep_cols chr, vector of column names to keep
#'
read_subset_consistent_data <- function(full_path, keep_cols, date_col = 'date',
                                        time_col = 'time', depth_col = 'dep.m', temp_col) {

  dat <- suppressMessages(readr::read_csv(full_path, name_repair = 'universal',
                                          locale = locale(encoding = "latin1"))) %>%
    # convert field names to lowercase because field names are consistent but
    # use of capitalization is not
    rename_all(tolower) %>%
    dplyr::select(all_of(tolower(keep_cols))) %>%
    mutate(DateTime = lubridate::mdy(get(date_col)),
           time = strftime(get(time_col), "%H:%M"),
           Timezone = NA,
           depth = get(depth_col),
           temp = get(temp_col),
           # extract lake id from file name because use of `Site.Name` field is inconsistent
           Missouri_ID = paste('Missouri_', extract_umo_lake_id(full_path), sep = ''),
           .keep = 'none')

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
      DateTime = extract_umo_date(full_path),
      time = NA, # skipping time for now, in the manual files the time moves around
      Timezone = NA,
      depth = as.numeric(.[[(depth_position - skip_cols)]]),
      temp = as.numeric(.[[(temp_position - skip_cols)]]),
      Missouri_ID = paste('Missouri_', extract_umo_lake_id(full_path), sep = ''),
      .keep = 'none'
    )

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
