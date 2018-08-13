#Takes 4 measurements a day at ~100,700,1300, 2000 hrs
#taking the third measurement of each day, since closest to noon
#Not sure what Vermillion DOW basin this comes from, but they all correspond
#to the same NHD lake, so just picking one
parse_Joes_Dock_2013 <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw_file <- data.table::fread(infile, skip = 1) %>% rename(temp = `Temp, °F (LGR S/N: 1109802, SEN S/N: 1109802)`)
  clean <- raw_file %>% mutate(temp = fahrenheit_to_celsius(temp),
                               DateTime = as.Date(`Date Time, GMT-05:00`, format = "%m/%d/%y"),
                               Depth = 0, DOW = '69037801') %>% filter(!is.na(temp)) %>%
    select(DateTime, Depth, temp, DOW)
  downsampled <- clean %>% group_by(DateTime) %>% slice(3)
  saveRDS(object = downsampled, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#same with picking an arbitrary Vermillion DOW
parse_Joes_Dock_Logger_2012 <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw <- readxl::read_excel(infile)
  clean <- raw %>% mutate(temp = fahrenheit_to_celsius(Temp),
                          Depth = 0, DOW = '69037801') %>%
    rename(DateTime = Date) %>% select(-Temp) %>%
    mutate(DateTime = as.Date(DateTime))
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#assuming this is the same instrument as the "open water" logger
#from 2/9/18 email
#keeping the afternoon measurement for downsampling (every 6 measurments)
parse_Lake_Vermilion_2016 <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1) %>% rename(DateTime = `Date Time, GMT-06:00`,
                                                        temp = "Temp, \xb0F (LGR S/N: 1161695, SEN S/N: 1161695)")
  clean <- raw %>% mutate(time = stringr::str_sub(as.character(DateTime), -4, -1),
                          temp = fahrenheit_to_celsius(temp),
                          DateTime = as.Date(DateTime, format = "%m/%d/%Y"),
                          Depth = 8/3.28, DOW = '69037801') %>%
    filter(time == "14:00") %>% select(DateTime, temp, Depth, DOW)
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#again keeping the afternoon measurements, assigning arbitrary Vermillion DOW
parse_Logger_Temps_2009_Joes_Dock <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1, col.names = c("num", "time",
                                                           "temp", "temp_avg"))
  clean <- raw %>% mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
                          time = substr(time, 10,20),
                          temp = fahrenheit_to_celsius(temp),
                          Depth = 0, DOW = '69037801') %>%
    filter(time == "02:34:47 PM") %>% select(DateTime, temp, Depth, DOW)
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#again keeping the afternoon measurements, assigning arbitrary Vermillion DOW
parse_Logger_Temps_2009_Open_Water <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1, col.names = c("num", "time",
                                                           "temp", "temp_avg",
                                                           paste0("V", 5:9)))
  clean <- raw %>% mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
                          time = substr(time, 10,20),
                          temp = fahrenheit_to_celsius(temp),
                          Depth = 8/3.28, DOW = '69037801') %>%
    filter(time == "03:52:33 PM") %>% select(DateTime, temp, Depth, DOW)
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#same assumptions as above
parse_Logger_Temps_2010_Open_Water <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1, col.names = c("num", "time",
                                                           "temp", "temp_avg"))
  clean <- raw %>% mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
                          time = substr(time, 10,20),
                          temp = fahrenheit_to_celsius(temp),
                          Depth = 8/3.28, DOW = '69037801') %>%
    #samping interval isn't quite even, so afternoon measurement isn't at the same time
    filter(!is.na(temp)) %>% group_by(DateTime) %>% filter(n() > 3) %>% slice(3) %>%
    select(DateTime, temp, Depth, DOW)
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#same assumptions as above
parse_Logger_Temps_2011_Open_Water <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw <- readxl::read_excel(infile, skip = 1) %>% rename(time = "Time, GMT-06:00",
                                                         temp = "Temp, °F",
                                                         avg_temp = "Avg: Temp, °F")
  clean <- raw %>% filter(!is.na(temp)) %>% mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
                                                   time = substr(time, 12,20),
                                                   temp = fahrenheit_to_celsius(temp),
                                                   Depth = 8/3.28, DOW = '69037801') %>% group_by(DateTime) %>%
    filter(n() > 3 & time == "15:21:27") %>% select(DateTime, temp, Depth, DOW)
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#again keeping the afternoon measurements, assigning arbitrary Vermillion DOW
parse_Open_Water_Logger_2013 <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1, col.names = c("num", "time",
                                                           "temp", "temp_avg",
                                                           paste0("V", 5:9)))
  clean <- raw %>% mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
                          time = substr(time, 10,20),
                          temp = fahrenheit_to_celsius(temp),
                          Depth = 8/3.28, DOW = '69037801') %>%
    filter(time == "02:36:01 PM") %>% select(DateTime, temp, Depth, DOW)
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Temp_Logger_Data_2015 <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw <- readxl::read_excel(infile, skip = 1) %>% rename(time = "Time, GMT-06:00",
                                                         temp = "Temp, °F",
                                                         avg_temp = "Avg: Temp, °F")
  clean <- raw %>% filter(!is.na(temp)) %>% mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
                                                   time = substr(time, 12,20),
                                                   temp = fahrenheit_to_celsius(temp),
                                                   Depth = 8/3.28, DOW = '69037801') %>% group_by(DateTime) %>%
    filter(n() > 3 & time == "03:08:30") %>% select(DateTime, temp, Depth, DOW)
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#again keeping the afternoon measurements, assigning arbitrary Vermillion DOW
parse_Vermilion_Logger_2014 <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, skip = 1, col.names = c("num", "time",
                                                           "temp", "temp_avg",
                                                           paste0("V", 5:9)))
  clean <- raw %>% mutate(DateTime = as.Date(time, format = "%m/%d/%y"),
                          time = substr(time, 10,20),
                          temp = fahrenheit_to_celsius(temp),
                          Depth = 8/3.28, DOW = '69037801') %>% filter(!is.na(temp)) %>%
    group_by(DateTime) %>% filter(n() > 3) %>%
    filter(time == "01:42:21 PM") %>% select(DateTime, temp, Depth, DOW)
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Verm_annual_tempDO_longformat <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw <- readxl::read_excel(infile)
  #multiple profiles/day — keeping WQ1 since it is deepest
  clean <- raw %>% mutate(DOW = '69037801') %>% filter(station == "WQ1") %>%
    rename(Depth = depth_m, DateTime = date) %>%
    select(DateTime, temp, Depth, DOW) %>% mutate(DateTime = as.Date(DateTime))
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
#similar format as above
parse_vermillion_repeated_tempDO_longformat <- function(inind, outind) {
  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)
  raw <- readxl::read_excel(infile)
  #multiple profiles/day — keeping WQ1 since it is deepest
  clean <- raw %>% mutate(DOW = '69037801') %>% filter(site == "wq1") %>%
    rename(Depth = depth, DateTime = datetext) %>% select(DateTime, temp, Depth, DOW) %>%
    mutate(DateTime = as.Date(DateTime))
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
