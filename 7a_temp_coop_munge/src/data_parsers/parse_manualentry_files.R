# parse "manual entry" files on gd that are being submitted by
# madeline - these have standard formatting so can use a single parser

parse_manualentry_files <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # read in data
  raw_dat <- readxl::read_excel(infile)

  # test for expected column structure
  expected_cols <- c("DOW", 'Date', "depth", "depth.units", "temperature", 'temp.units')

  if (!all(expected_cols %in% names(raw_dat))) {
    stop(paste('File', infile, 'did not have the expected data structure. Please review data file/parser.'))
  }

  # test for expected values in units columns
  expected_temp_units <- c("C", "F")
  expected_depth_units <- c('f', 'm')

  # reformat/munge data
  dat_clean <- raw_dat %>%
    mutate(depth = ifelse(depth.units %in% 'f', depth/3.28, depth),
           temp = ifelse(temp.units %in% "F", fahrenheit_to_celsius(temperature), temperature)) %>%
    select(DateTime = Date, depth, temp, DOW)

  saveRDS(object = dat_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
