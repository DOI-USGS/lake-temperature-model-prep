# parse WI files

parse_WI_Profile_Data_1995to2015 <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # read in data
  raw_dat <- readxl::read_excel(infile, sheet = 'temperature')

  # reformat/munge data
  dat_clean <- raw_dat %>%
    mutate(depth = ifelse(depth.units %in% 'f', depth/3.28, depth),
           temp = ifelse(temp.units %in% "F", fahrenheit_to_celsius(temperature), temperature)) %>%
    select(DateTime = date, depth, temp, WBIC)

  saveRDS(object = dat_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
