parse_micorps_secchi_temp_DO_alltiers <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # read in data
  raw_dat <- readxl::read_excel(infile, sheet = 'oxygen_temp')

  # clean data

  dat_clean <- raw_dat %>%
    dplyr::select(DateTime =`Date Sampled`, depth = `Depth (feet)`,
           temp = starts_with('Temp'), id = `STORETID`, time = `Time Sampled`) %>%
    mutate(DateTime = as.Date(DateTime),
           depth = convert_ft_to_m(as.numeric(depth)),
           temp = as.numeric(temp),
           time = substr(time, 0, 5),
           timezone = 'EST/EDT') %>%
    arrange(id, DateTime) %>%
    filter(!is.na(depth),
           !is.na(temp))

  saveRDS(object = dat_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Michigan_micorps_data <- function(inind, outind) {

  parse_micorps_secchi_temp_DO_alltiers(inind, outind)
}
