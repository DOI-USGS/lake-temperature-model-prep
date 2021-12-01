
# Parse the ~300 files within the `Bull_Shoals_Lake_DO_and_Temp.zip` file

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
                    # TODO: Don't manually add the nhdhr id
                    # Only doing now bc we don't have hypso for
                    # Bull Shoals yet and I found this nhdhr match:
                    # https://github.com/USGS-R/lake-temperature-model-prep/blob/master/4_params_munge.yml#L60
                    id = 'nhdhr_12003253',
                    depth = `Depth (ft)`) %>%
      dplyr::rename(temp = "Temp (°C)") %>%

      # Replace "Surface" depth and convert depths to numeric
      mutate(depth = as.numeric(ifelse(depth == "Surface", 0, depth))) %>%

      # Convert depth from feet to meters
      mutate(depth = depth * 0.3048) %>%

      # Keep just the columns we need
      dplyr::select(DateTime, depth, temp, id, site)

  }) %>%  bind_rows() %>%
    arrange(site, DateTime, depth)

  saveRDS(object = data_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}

# Thanks https://stackoverflow.com/questions/34024654/reading-rdata-file-with-different-encoding
fix_colname_encoding <- function(df, new_encoding = c("UTF-8", "latin1")) {
  cnames <- colnames(df)
  Encoding(cnames) <- new_encoding
  colnames(df) <- cnames
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
