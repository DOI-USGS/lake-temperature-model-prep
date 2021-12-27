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
      id = 'nhdhr_105341319',
      site = Site
    ) %>%
    dplyr::select(DateTime, time, Timezone, depth, temp, id, site)

  # save data
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}

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


