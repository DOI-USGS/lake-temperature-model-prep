parse_mndnr_files <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)


  dat_sheets <- readxl::excel_sheets(infile)
  temp_sheets <- grep('Temp', dat_sheets, value = TRUE)

  all_dat <- data.frame()
  for (i in 1:length(temp_sheets)) {

    dat_raw <- read_excel(infile, sheet = temp_sheets[i])

    dat_cleaned <- dat_raw %>%
      dplyr::mutate(DateTime = as.Date(Date),
                    time = ifelse(is.na(Time), NA, format(Time, '%H:%M')),
                    timezone = ifelse(is.na(time), NA, 'CST/CDT')) %>%
      dplyr::select(DateTime, time, timezone,
             depth = `Depth (m)`,
             temp = dplyr::starts_with('Temp'),
             site = Site) %>%
      dplyr::mutate(sheet = temp_sheets[i]) %>%
      dplyr::arrange(site, DateTime, depth) %>%
      dplyr::filter(!is.na(depth), !is.na(temp))

    all_dat <- dplyr::bind_rows(all_dat, dat_cleaned)
  }

  # manually found DOW numbers from here: https://maps2.dnr.state.mn.us/ewr/fom/mapper.html?layers=lakes%20streams%20wshd_lev01py3%20occurrences
  all_dat <- mutate(all_dat, DOW = case_when(
    grepl('Rainy', sheet) ~ '69069400',
    grepl('Crane', sheet) ~ '69061600',
    grepl('Kab', sheet) ~ '69084500',
    grepl('Vermilion', sheet) ~ '69060800',
    grepl('Namakan', sheet) ~ '69069300',
    grepl('Sand', sheet) ~ '69061700'
  ))

  all_dat <- dplyr::select(all_dat, DateTime, time, depth, temp, DOW, site)

  saveRDS(object = all_dat, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)


}

# parser and helper functions for temp_DO_PCA.zip -----------
parse_temp_DO_PCA <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # Unzip the file with all of Bull Shoals, then cleanup unzipped files
  # which aren't needed externally before leaving the function.
  unzip_dir <- tempdir()
  files_from_zip <- unzip(infile, exdir = unzip_dir)
  on.exit(unlink(unzip_dir, recursive = TRUE))

  clean <- files_from_zip %>%
    map_dfr(~ clean_temp_do_pca_files(.x))

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

clean_temp_do_pca_files <- function(file_path) {
  out <- readr::read_csv(file_path, , col_types = 'cnccnccnccccncc') %>%
    dplyr::filter(Parameter == 'Temperature, water') %>%
    dplyr::select(Lake_Name, DOWLKNUM, Date, Time,
                  Result, Upper_Depth) %>%
    dplyr::rename(DateTime = Date,
                  time = Time,
                  depth = Upper_Depth,
                  temp = Result,
                  DOW = DOWLKNUM,
                  site = Lake_Name
    ) %>%
    mutate(DateTime = as.Date(strptime(DateTime, "%m/%d/%Y")),
           time = substr(as.character(time), 1, 5))

  return(out)
}


# parser and helper functions for Long29016100.zip -----------
parse_Long_29016100 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # Unzip the file with all of Bull Shoals, then cleanup unzipped files
  # which aren't needed externally before leaving the function.
  unzip_dir <- tempdir()
  files_from_zip <- unzip(infile, exdir = unzip_dir)
  on.exit(unlink(unzip_dir, recursive = TRUE))


  clean <- files_from_zip %>%
    purrr::map_dfr(~ clean_Long29016100(file_path = .x,
                                        skip = 2, date_cell = 'E1',
                                        omit_sheets = c('Summary', 'Depths',
                                                        'Layers', '8-12-16 (Sta 1 So.)',
                                                        '8-12-16 (Sta 3 No.)')))

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#' @param file_path chr, full file path for file
#' @param skip int, number of rows to skip
#' @param date_cell chr, Excel-style cell ID for the sample date
#' @param omit_sheets chr, vector of sheets that should be omitted if encountered
#'
clean_Long_29016100 <- function(file_path, skip = 2,
                               date_cell = 'E1', omit_sheets = NULL) {

  # read sheets and remove omitted sheets
  sheets <- readxl::excel_sheets(file_path) %>%
    .[!(. %in% omit_sheets)]

  clean <- sheets %>%
    map_dfr(~ build_sheet_data(file_path = file_path, sheet = .x,
                               date_cell = date_cell, skip = skip))
}

#' @param file_path chr, full file path for file
#' @param sheet chr, name of the sheet that should be read in
#' @param skip int, number of rows to skip
#' @param date_cell chr, Excel-style cell ID for the sample date
#'
build_sheet_data <- function(file_path, sheet, date_cell, skip) {
  # extract sample date
  sample_date <- readxl::read_excel(path = file_path,
                                    sheet = sheet,
                                    range = date_cell,
                                    col_names = F) %>% dplyr::pull()

  # read sheet data, tack on date and DOW
  dat <- readxl::read_excel(path = file_path, sheet = sheet, skip = skip) %>%
    dplyr::select(matches('depth m|oC'))

  names(dat) <- c('depth', 'temp') # doing it this way because the name of the depth column varies between datasets, but is always in the first position

  dat <- dat %>%
    mutate(DateTime = as.Date(sample_date),
           DOW = 29016100) %>%
    na.omit

  return(dat)
}


