# parse WI files

parse_WI_Profile_Data_1995to2015 <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # read in data
  raw_dat <- readxl::read_excel(infile, sheet = 'temperature')

  # group by date and depth and see if there are duplicate values
  # there are, but time stamp is not helping. Take first value of each lake/date/depth combination
  dat_clean <- raw_dat %>%
    mutate(DateTime = as.Date(date),
           depth = ifelse(depth.units %in% 'f', feet_to_meters(depth), depth),
           temp = ifelse(temp.units %in% "F", fahrenheit_to_celsius(temperature), temperature),
           time = format(date, format = '%H:%M'),
           timezone = 'CST/CDT') %>%
    #group_by(DateTime, WBIC, depth) %>%
    #summarize(temp = first(temp)) %>%
    select(DateTime, time, timezone, depth, temp, WBIC) %>%
    filter(!is.na(depth), !is.na(temp)) %>%
    arrange(WBIC, DateTime)

  saveRDS(object = dat_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
