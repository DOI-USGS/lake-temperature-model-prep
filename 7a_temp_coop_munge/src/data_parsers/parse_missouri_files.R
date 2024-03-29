
parse_20190409_DATA_with_all_depths <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat_raw <- readr::read_csv(infile)

  # split date and time
  dat_datetime_split <- stringr::str_split_fixed(dat_raw$Timestamp, " ", 2) %>%
    as_tibble %>%
    rename('DateTime' = V1, 'time' = V2)

  data_clean <- dat_raw %>%
    cbind(., dat_datetime_split) %>%

    dplyr::mutate(DateTime = as.Date(DateTime),
           depth = convert_ft_to_m(Depth),
           temp = fahrenheit_to_celsius(`Temperature (F)`),
           Missouri_ID = 'Missouri_100', # used for crosswalk
           site = Site) %>%
    dplyr::select(DateTime, time, depth, temp, Missouri_ID, site)

  saveRDS(object = data_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}

parse_Bull_Shoals_and_LOZ_profile_data_LMVP <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat_raw <- readxl::read_xlsx(infile)

  data_clean <- dat_raw %>%
    dplyr::mutate(DateTime = as.Date(Date),
                  depth = `Depth m`,
                  temp = C,
                  Missouri_ID = Lake,
                  site = `Name and Site`,
                  Missouri_ID = case_when(
                    # values from Univ MO xwalk (https://drive.google.com/file/d/11w6-LXCDSDCjipFYPxUgJyf7YB9BtXYR/view?usp=sharing)
                    Lake == 'Bull Shoals' ~ 'Missouri_100',
                    Lake == 'Lake of the Ozarks' ~ 'Missouri_149'
                  )
                ) %>%
    dplyr::select(DateTime, depth, temp, Missouri_ID, site)

  saveRDS(object = data_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}


# Parse the 14 files in Temp_DO_BSL_MM_DD_YYY
parse_Temp_DO_BSL_MM_DD_YYYY <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # Unzip the file with all of Bull Shoals, then cleanup unzipped files
  # which aren't needed externally before leaving the function.
  unzip_dir <- tempdir()
  files_from_zip <- unzip(infile, exdir = unzip_dir)
  on.exit(unlink(unzip_dir, recursive = TRUE))

  data_clean <- files_from_zip %>%
    purrr::map_df(~ read_bullshoals_data(.,
                                         new_col_names = c('DateTime', 'DO',
                                                           'temp', 'site',
                                                           'depth', 'unit_id')))

  saveRDS(object = data_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

read_bullshoals_data <- function(file_path, new_col_names, keep_cols) {

  # identify sheets with data
  list_sheets <- readxl::excel_sheets(file_path) %>%
    .[grepl('^Pt', .)]

  # read in data and clean
  clean <- list_sheets %>%
    purrr::map_df(~ readxl::read_xlsx(path = file_path, sheet = .x)) %>%
    purrr::discard(~all(is.na(.x))) %>%  # remove columns with all NA
    dplyr::filter(!is.na(Timestamp)) %>% # remove rows without timestamp
    dplyr::rename_with(~ new_col_names) %>%
    mutate(
      Timezone = c('CDT/CST'),
      temp = fahrenheit_to_celsius(temp),
      depth = -1 * depth,
      Missouri_ID = 'Missouri_100' # value from Univ MO xwalk (https://drive.google.com/file/d/11w6-LXCDSDCjipFYPxUgJyf7YB9BtXYR/view?usp=sharing)
    ) %>%
    dplyr::select(DateTime, Timezone, depth, temp, site, Missouri_ID)

  # a final fix on DateTime to accommodate
  # one file `Temp _ DO BSL 09-26-2019.xlsx`
  if(is.character(clean$DateTime)){clean$DateTime <- mdy_hm(clean$DateTime)}
  clean$DateTime <- as.Date(clean$DateTime)

  return(clean)
}






# Parse the ~300 files within the `Bull_Shoals_Lake_DO_and_Temp.zip` file -------
parse_Bull_Shoals_Lake_DO_and_Temp <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # Unzip the file with all of Bull Shoals, then cleanup unzipped files
  # which aren't needed externally before leaving the function.
  unzip_dir <- tempdir()
  files_from_zip <- unzip(infile, exdir = unzip_dir)
  on.exit(unlink(unzip_dir, recursive = TRUE))

  # Read and clean each file (create on big file)
  data_clean <- purrr::map(files_from_zip, function(fn) {

    # Only read in the data part of the Excel file
    # Assumes each file has only these rows/columns
    # of data (which looked correct based on my spot check)
    readxl::read_excel(fn, range = "A7:D27") %>%

      # Struggles with the special symbols in the column names
      # due to encoding issues. See details in the `scipiper`
      # issue: https://github.com/USGS-R/scipiper/issues/151
      fix_colname_encoding("latin1") %>%
      # Reformat columns and extract info we need from Excel file
      dplyr::mutate(DateTime = extract_bullshoals_date(fn),
                    site = extract_bullshoals_site(fn),
                    # `Missouri_ID` manually added after receiving
                    # xwalk table from Univ of Missouri that included
                    # Bull Shoals (https://drive.google.com/file/d/11w6-LXCDSDCjipFYPxUgJyf7YB9BtXYR/view?usp=sharing)
                    Missouri_ID = 'Missouri_100',
                    depth = `Depth (ft)`) %>%
      dplyr::rename(temp = "Temp (°C)") %>%

      # Replace "Surface" depth and convert depths to numeric
      dplyr::mutate(depth = as.numeric(ifelse(depth == "Surface", 0, depth))) %>%

      # Convert depth from feet to meters
      dplyr::mutate(depth = convert_ft_to_m(depth)) %>%

      # Keep just the columns we need
      dplyr::select(DateTime, depth, temp, Missouri_ID, site)

  }) %>%  bind_rows() %>%
    dplyr::arrange(site, DateTime, depth)

  saveRDS(object = data_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}

# Thanks https://stackoverflow.com/questions/34024654/reading-rdata-file-with-different-encoding
fix_colname_encoding <- function(df, new_encoding = c("UTF-8", "latin1")) {
  if(Sys.info()[['sysname']] == "Windows") {
    cnames <- colnames(df)
    Encoding(cnames) <- new_encoding
    colnames(df) <- cnames
  }
  return(df)
}

extract_bullshoals_date <- function(fn) {
  # Extract the date from the first row of the first column and format as a Date
  date_raw <- suppressMessages(readxl::read_excel(fn, range = "A1:A1", col_names=FALSE))
  date_clean <- as.Date(gsub("Date:  ", "", date_raw[[1]]), format = "%m/%d/%Y")

  # Could include times, but would need to pick to use the start or end time and
  # then extract, format, and add to the Date
  date_ready <- date_clean

  return(date_ready)
}

extract_bullshoals_site <- function(fn) {
  # Extract the site name from the first row
  site_raw <- suppressMessages(readxl::read_excel(fn, range = "F1:G1", col_names=FALSE))

  # There is one day where the site name was put in a different column. Handle this case:
  if(is.na(site_raw[[2]])) {
    site_ready <- gsub("Site Name:", "", site_raw[[1]]) %>% stringr::str_trim()
  } else {
    site_ready <- site_raw[[2]]
  }

  return(site_ready)
}
