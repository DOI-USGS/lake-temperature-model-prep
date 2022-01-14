#Takes 4 measurements a day at ~100,700,1300, 2000 hrs
#taking the third measurement of each day, since closest to noon
#Not sure what Vermillion DOW basin this comes from, but they all correspond
#to the same NHD lake, so just picking one
# will maintain site IDs so clear that these should be averaged
parse_Joes_Dock_2013 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  raw_file <- data.table::fread(infile, skip = 1) %>%
    rename(temp = starts_with("Temp,"))

  clean <- raw_file %>%
    mutate(temp = fahrenheit_to_celsius(temp),
           DateTime = as.Date(`Date Time, GMT-05:00`, format = "%m/%d/%y"),
           time = format(strptime(`Date Time, GMT-05:00`, format = '%m/%d/%y %I:%M:%S %p', tz = 'Etc/GMT+5'), '%H:%M'),
           timezone = 'GMT-5',
           depth = 0,
           DOW = '69037801',
           site = "Joe's Dock") %>%
    filter(!is.na(temp)) %>%
    dplyr::select(DateTime, time,  timezone, depth, temp, DOW, site)

  #downsampled <- clean %>%
  #  group_by(DateTime) %>%
  #  slice(3)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#same with picking an arbitrary Vermillion DOW
# no times in dock logger 2012 file
parse_Joes_Dock_Logger_2012 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- readxl::read_excel(infile)
  clean <- raw %>%
    mutate(temp = fahrenheit_to_celsius(Temp),
           depth = 0,
           DOW = '69037801') %>%
    rename(DateTime = Date) %>%
    dplyr::select(-Temp) %>%
    mutate(DateTime = as.Date(DateTime),
           site = "Joe's Dock")
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#assuming this is the same instrument as the "open water" logger
#from 2/9/18 email
parse_Lake_Vermilion_2016 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1) %>%
    rename(DateTime = `Date Time, GMT-06:00`,
           temp = "Temp, \xb0F (LGR S/N: 1161695, SEN S/N: 1161695)")

  clean <- raw %>%
    mutate(time = strftime(as.POSIXct(DateTime, format = "%m/%d/%Y %H:%M"), '%H:%M'),
           temp = fahrenheit_to_celsius(temp),
           DateTime = as.Date(DateTime, format = "%m/%d/%Y"),
           depth = convert_ft_to_m(8),
           DOW = '69037801',
           timezone = 'GMT-5',
           site = 'open water logger') %>%
    #filter(time == "14:00") %>%
    dplyr::select(DateTime, time, timezone, temp, depth, DOW, site)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#again keeping the afternoon measurements, assigning arbitrary Vermillion DOW
parse_Logger_Temps_2009_Joes_Dock <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1, col.names = c("num", "time",
                                                           "temp", "temp_avg"))
  clean <- raw %>%
    mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
           time = format(strptime(time, format = '%m/%d/%y %I:%M:%S %p', tz = 'Etc/GMT+5'), '%H:%M'),
           timezone = 'GMT-5',
           temp = fahrenheit_to_celsius(temp),
           depth = 0,
           DOW = '69037801',
           site = "Joe's Dock") %>%
    #filter(time == "02:34:47 PM") %>%
    filter(!is.na(temp)) %>%
    dplyr::select(DateTime, time, timezone, temp, depth, DOW, site)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#again keeping the afternoon measurements, assigning arbitrary Vermillion DOW
parse_Logger_Temps_2009_Open_Water <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1, col.names = c("num", "time",
                                                           "temp", "temp_avg",
                                                           paste0("V", 5:9)))
  clean <- raw %>%
    mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
           time = format(strptime(time, format = '%m/%d/%y %I:%M:%S %p', tz = 'Etc/GMT+5'), '%H:%M'),
           temp = fahrenheit_to_celsius(temp),
           depth = convert_ft_to_m(8),
           DOW = '69037801',
           timezone = 'GMT-5',
           site = 'open water logger') %>%
    #filter(time == "03:52:33 PM") %>%
    dplyr::select(DateTime, time, timezone, temp, depth, DOW, site)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#same assumptions as above
parse_Logger_Temps_2010_Open_Water <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1, col.names = c("num", "time",
                                                           "temp", "temp_avg"))
  clean <- raw %>%
    mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
           time = format(strptime(time, format = '%m/%d/%y %I:%M:%S %p', tz = 'Etc/GMT+6'), '%H:%M'),
           temp = fahrenheit_to_celsius(temp),
           depth = convert_ft_to_m(8),
           DOW = '69037801',
           timezone = 'GMT-6',
           site = 'open water logger') %>%
    #samping interval isn't quite even, so afternoon measurement isn't at the same time
    filter(!is.na(temp)) %>%
    dplyr::select(DateTime, time, timezone, temp, depth, DOW, site)
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#same assumptions as above
parse_Logger_Temps_2011_Open_Water <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  raw <- readxl::read_excel(infile, skip = 1) %>%
    rename(time = "Time, GMT-06:00",
           temp = starts_with("Temp,"),
           avg_temp = starts_with("Avg: Temp"))

  clean <- raw %>%
    filter(!is.na(temp)) %>%
    mutate(DateTime = as.Date(time, format = "%Y-%m-%d"),
           time = format(strptime(time, format = '%Y-%m-%d %H:%M:%S', tz = 'Etc/GMT+6'), '%H:%M'),
           temp = fahrenheit_to_celsius(temp),
           depth = convert_ft_to_m(8),
           DOW = '69037801',
           timezone = 'GMT-6',
           site = 'open water logger') %>%
    #group_by(DateTime) %>%
    #filter(n() > 3 & time == "15:21:27") %>%
  dplyr::select(DateTime, time, timezone, temp, depth, DOW, site)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#again keeping the afternoon measurements, assigning arbitrary Vermillion DOW
parse_Open_Water_Logger_2013 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1, col.names = c("num", "time",
                                                           "temp", "temp_avg",
                                                           paste0("V", 5:9)))
  clean <- raw %>%
    mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
           time = format(strptime(time, format = '%m/%d/%y %I:%M:%S %p', tz = 'Etc/GMT+6'), '%H:%M'),
           timezone = 'GMT-6',
           temp = fahrenheit_to_celsius(temp),
           depth = convert_ft_to_m(8),
           DOW = '69037801',
           site = 'open water logger') %>%
    #filter(time == "02:36:01 PM") %>%
    dplyr::select(DateTime, time, timezone, temp, depth, DOW, site) %>%
    filter(!is.na(temp))

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Temp_Logger_Data_2015 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  raw <- readxl::read_excel(infile, skip = 1)

  clean <- raw %>%
    rename(temp = starts_with("Temp,"),
           avg_temp = starts_with("Avg: Temp")) %>%
    mutate(time = format(`Time, GMT-06:00`, '%H:%M'))

  clean <- clean %>%
    filter(!is.na(temp)) %>%
    mutate(DateTime = as.Date(`Time, GMT-06:00`, format = "%m/%d/%y"),
           temp = fahrenheit_to_celsius(temp),
           depth = convert_ft_to_m(8), DOW = '69037801',
           timezone = 'GMT-6',
           site = 'open water logger') %>%
    #group_by(DateTime) %>%
    #filter(n() > 3 & time == "03:08:30") %>%
    dplyr::select(DateTime, time, timezone, temp, depth, DOW, site)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#again keeping the afternoon measurements, assigning arbitrary Vermillion DOW
parse_Vermilion_Logger_2014 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1, col.names = c("num", "time",
                                                           "temp", "temp_avg",
                                                           paste0("V", 5:9)))
  clean <- raw %>%
    mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
           time = format(strptime(time, format = '%m/%d/%y %I:%M:%S %p', tz = 'Etc/GMT+6'), '%H:%M'),
           temp = fahrenheit_to_celsius(temp),
           depth = convert_ft_to_m(8),
           DOW = '69037801',
           timezone = 'GMT-6',
           site = 'open water logger') %>% # assuming to be similar site to open water loggers from other years
    filter(!is.na(temp)) %>%
    #group_by(DateTime) %>% filter(n() > 3) %>%
    #filter(time == "01:42:21 PM") %>%
    dplyr::select(DateTime, time, timezone, temp, depth, DOW, site)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Verm_annual_tempDO_longformat <- function(inind, outind) {
  # no time
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- readxl::read_excel(infile)
  #multiple profiles/day — keeping WQ1 since it is deepest
  clean <- raw %>%
    mutate(DOW = '69037801',
           site = toupper(station)) %>%
    #filter(station == "WQ1") %>%
    rename(depth = depth_m, DateTime = date) %>%
    dplyr::select(DateTime, temp, depth, DOW, site) %>%
    mutate(DateTime = as.Date(DateTime))

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
#similar format as above
parse_vermillion_repeated_tempDO_longformat <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- readxl::read_excel(infile)
  #multiple profiles/day — keeping WQ1 since it is deepest
  clean <- raw %>%
    mutate(DOW = '69037801', site = toupper(site)) %>%
    #filter(site == "wq1") %>%
    rename(DateTime = datetext) %>%
    dplyr::select(DateTime, temp, depth, DOW, site) %>%
    mutate(DateTime = as.Date(DateTime))

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
