parse_1945_2020_All_MNDNR_MPCA_Temp_DO_Profiles <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  readxl::read_excel(infile) %>%
    dplyr::mutate(DateTime = as.Date(DATE)) %>%
    dplyr::select(DateTime, depth = DEPTH_M, temp = TEMP_C, DOW) %>%
    dplyr::arrange(DateTime, depth) %>%
    dplyr::filter(!is.na(depth), !is.na(temp)) %>%
    saveRDS(file = outfile)

  sc_indicate(ind_file = outind, data_file = outfile)

}

parse_DO_and_Temp_data_for_all_LF_lakes <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  readxl::read_excel(infile, range = "A2:I30000", col_types = "text") %>%
    filter(!is.na(temperature)) %>%
    mutate(DateTime = as.Date('1900-01-01') + as.numeric(Date),
           depth = case_when(
             grepl('surface', tolower(depth)) ~ 0,
             grepl('bottom', tolower(depth)) ~ NA_real_,
             depth.units == "F" ~ as.numeric(depth) * 0.3048,
             depth.units == "m" ~ as.numeric(depth),
             TRUE ~ NA_real_
           ),
           temp = case_when(
             C == 'C' ~ as.numeric(temperature),
             TRUE ~ NA_real_
           )) %>%
    dplyr::select(DateTime, depth, temp, DOW) %>%
    dplyr::arrange(DateTime, depth) %>%
    saveRDS(file = outfile)

  sc_indicate(ind_file = outind, data_file = outfile)
}
