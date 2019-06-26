parse_mendota_daily_buoy <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw_file <- data.table::fread(infile)
  #flag code definitions are in the EML format on the UW limno data site
  #https://lter.limnology.wisc.edu/data
  clean <- raw_file %>%
    filter(!flag_wtemp %in% c("A11N", "D", "H")) %>%
    rename(DateTime = sampledate, temp = wtemp) %>%
    mutate(DateTime = as.Date(DateTime), UWID = "ME") %>%
    mutate(WBIC = '805400') %>%
    dplyr::select(DateTime, depth, temp, WBIC)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_long_term_ntl <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw_file <- data.table::fread(infile, select = c("lakeid", "sampledate",
                                                   "depth", "wtemp"))
  clean <- raw_file %>%
    rename(UWID = lakeid, DateTime = sampledate,
           temp = wtemp) %>%
    mutate(DateTime = as.Date(DateTime)) %>%
    mutate(WBIC = case_when(
      UWID == 'AL' ~ '2332400',
      UWID == 'BM' ~ '1835300',
      UWID == 'CB' ~ '2017500',
      UWID == 'CR' ~ '1842400',
      UWID == 'FI' ~ '985100',
      UWID == 'ME' ~ '805400',
      UWID == 'MO' ~ '804600',
      UWID == 'SP' ~ '1881900',
      UWID == 'TB' ~ '2014700',
      UWID == 'TR' ~ '2331600',
      UWID == 'WI' ~ '805000')) %>%
    dplyr::select(DateTime, depth, temp, WBIC)

  saveRDS(object = clean, file = outfile)
  scipiper::sc_indicate(outind, data_file = outfile)
}

parse_mendota_temps_long <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw_file <- data.table::fread(infile, select = c("sampledate", "depth", "watertemp"))

  clean <- raw_file %>%
    mutate(WBIC = "805400") %>%
    rename(DateTime = sampledate, temp = watertemp) %>%
    filter(depth != "MUD") %>%
    mutate(DateTime = as.Date(DateTime), depth = as.numeric(depth)) %>%
    dplyr::select(DateTime, depth, temp, WBIC)

  saveRDS(object = clean, file = outfile)
  sc_indicate(outind, data_file = outfile)
}
