# Parse NorkfolkReservoir_AR_monthlyTempDO files ----------------------
parse_NorkfolkReservoir_AR_monthlyTempDO(inind, outind) {
  infile <- scipiper::sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- scipiper::as_data_file(outind)

  dat_raw <- readxl::read_xlsx(infile) %>%
    dplyr::arrange(Timestamp)

  clean <- dat_raw %>%
    dplyr::arrange(Timestamp) %>%
    dplyr::select(Timestamp, `Temperature (C)`, `Unit ID`) %>%
    dplyr::mutate(DateTime = as.Date(Timestamp),
                  time = strftime(Timestamp, format = '%H:%M', tz = 'GMT'),
                  Norfork_ID = paste0('Norfork_', `Unit ID`)) %>%
    dplyr::rename(temp = `Temperature (C)`) %>%
    dplyr::select(DateTime, time, temp, Norfork_ID)

  # depth is manually assigned in the spreadsheet - not all files in the raw
  # not all files in the raw data make it into
  # the profile analysis
  clean <- remove_incomplete_profiles(clean)
  clean$depth <- add_depth_data(clean)
  clean$depth <- convert_ft_to_m(clean$depth)

  return(clean)

}

# helper fxns for `parse_NorkfolkReservoir_AR_monthlyTempDO` ------

#' Remove days with incomplete profiles
#'
#' These Norfork data sets contain `Raw Data` and month-by-month data. It
#' was assumed that the month-by-month data was the best quality, but it was
#' also challenging to parse. Therefore, we took the approach of loading
#' in the raw data sheet and then removing datasets that we not in the
#' month-by-month data.
#'
#' @param df a data.frame of Norfolk Reservoir data that contains incomplete WQ profiles
#'
remove_incomplete_profiles <- function(df) {
  suspicious_dates <- df %>%
    dplyr::group_by(DateTime) %>%
    dplyr::tally() %>%
    dplyr::filter(n < 15) %>% # threshold was based on manual data inspection
    dplyr::pull(DateTime)

  df_filtered <- df %>%
    dplyr::filter(!(DateTime %in% suspicious_dates))

  return(df_filtered)
}

#' Add depth data to Norfolk Data
#'
#' The Norfolk data in the `Raw Data` tab did not contain depths. However,
#' reviewing the month-by-month data, you can see that depths are measured
#' in 5 ft increments. This function counts the number of records in a given
#' day and assigns depths to the profile.
#'
#' @param df a data.frame of Norfolk Reservoir data that contains complete profiles with no depth data
#'
add_depth_data <- function(df) {
  depth_vectors <- df %>%
    dplyr::group_by(DateTime) %>%
    dplyr::tally

  # Create a depth vector for each unique day
  # I chose to name the `out` df and to combine the list
  # of vectors at the end for potential troubleshooting down the line
  depth_vec_by_day <- depth_vectors %>%
    dplyr::pull(n) %>%
    # need to subtract 1 from .x because `0` is the first value
    purrr::map(~ seq(from = 0, to = ((.x -1) * 5), by = 5))
  names(depth_vec_by_day) <- depth_vectors$DateTime

  out <- depth_vec_by_day %>% unlist

  return(out)
}


# Parse Norkfolk_DAM-YYYY and Norfolk_62BRG-YYYY files ----------------------
parse_norfork_files <- function(inind, outind) {
  infile <- scipiper::sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- scipiper::as_data_file(outind)

  # load data: identify sheets in data, read in raw data by sheet,
  # and combine into one data set
  dat_raw <- lapply(infile, read_norfork_xlsx) %>%
    bind_rows()

  # format data for pipeline
  clean <- dat_raw %>%
    dplyr::filter(!is.na(Timestamp)) %>%
    dplyr::mutate(
      DateTime = as.Date(Timestamp),
      time = format(Timestamp, "%H:%M"),
      Timezone = format(Timestamp, "%Z"),
      depth = convert_ft_to_m(`...10`) * -1, # depth is referenced from water surface
      temp = `Temperature (C)`,
      # One lake with two sites. Manually set from
      # https://github.com/USGS-R/lake-temperature-model-prep/blob/0c037ddce5bdab7a7eb3a6ad07c0837972bddcd1/4_params_munge.yml#L63
      Norfork_ID = paste("Norfork_", `Unit ID`, sep = ''),
      site = Site
    ) %>%
    dplyr::select(DateTime, time, Timezone, depth, temp, Norfork_ID, site)

  # save data
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}

# Helper fxns for  Norkfolk_DAM-YYYY and Norfolk_62BRG-YYYY files -----------

#' Load Norfork xlsx data
#'
#' Reads raw data from Norkfork sites, skipping the first two sheets
#'
#' @param file_in full file path for Norfork data xlsx
#' @param skip number of sheets to skip, starting from 1
#'
read_norfork_xlsx <- function(file_in, skip = 2) {

  start <- skip + 1

  file_in %>%
    readxl::excel_sheets() %>%
    .[start:length(.)] %>%
    rlang::set_names() %>%
    purrr::map_df(~ read_excel(path = file_in, sheet = .x, range = 'A1:I50'),
                  .id = 'sheet')
}


