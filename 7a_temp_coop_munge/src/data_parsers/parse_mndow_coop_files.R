#parse various minnesota state agency data files
parse_MPCA_temp_data_all <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw_file <- data.table::fread(infile, colClasses = c(DOW="character"),
                                select = c("SAMPLE_DATE", "START_DEPTH", "SAMPLETIME", "DEPTH_UNIT",
                                           "RESULT_NUMERIC", "RESULT_UNIT", "DOW"))


  assert_that(unique(raw_file$RESULT_UNIT) == "deg C")
  #some measurements missing depth unit
  clean <- raw_file %>%
    filter(DEPTH_UNIT == "m") %>%
    dplyr::select(-RESULT_UNIT, -DEPTH_UNIT) %>%
    mutate(SAMPLE_DATE = as.Date(SAMPLE_DATE, format = "%m/%d/%Y"),
           timezone = 'CST/CDT') %>%
    rename(DateTime = SAMPLE_DATE, time = SAMPLETIME, depth = START_DEPTH, temp = RESULT_NUMERIC)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Water_Temp <- function(inind, outind){
  dat <- feather::read_feather(sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml'))
  outfile <- as_data_file(outind)

  data_out <- dat %>%
    filter(FlagV == "P", FlagG == "P", FlagS == 'P', FlagR == 'P', FlagF == 'P') %>%
    mutate(POSIXct = as.POSIXct(Date_Time, "%m/%d/%y %H:%M:%S", tz = "UTC"),
           time = strftime(POSIXct, format = '%H:%M'),
           DateTime = as.Date(POSIXct),
           DOW = DOWLKNUM, depth = Depth_m, temp = Water_Temp_C,
           timezone = "UTC", site = String) %>%
    dplyr::select(DateTime, time, timezone, depth, temp, DOW, site) %>%
    arrange(DOW, DateTime, time, depth)

  saveRDS(object = data_out, file = outfile)
  sc_indicate(ind_file = outind, data_file =  outfile)
}

parse_URL_Temp_Logger_2006_to_2017 <- function(inind, outind) {
  df <- feather::read_feather(sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml'))
  outfile <- as_data_file(outind)

  #3 tables in database
  #need to add DOW for Red lake, add depth in m, convert to deg C
  #time is stored in a separate column, but it seems to have a date
  #added starting from 12/30/99?
  df_clean <- df %>%
    mutate(DOW = "04003501",
           temp = fahrenheit_to_celsius(WaterTempF),
           depth = convert_ft_to_m(11),
           DateTime = as.Date(Date, format = "%m/%d/%y"),
           time = strftime(Time, format = '%H:%M'),
           timezone = 'CST/CDT') %>%
    dplyr::select(DateTime, time, timezone, depth, temp, DOW) %>%
    arrange(DateTime)

  saveRDS(object = df_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file =  outfile)
}

parse_MN_fisheries_all_temp_data_Jan2018 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile, colClasses = c(DOW="character"))
  #convert to meters depth and deg C temp
  # no time data, not clear if repeated values are from multiple sites or multiple times
  clean <- raw %>% mutate(temp = fahrenheit_to_celsius(TEMP_F),
                          depth = convert_ft_to_m(DEPTH_FT),
                          DateTime = as.Date(SAMPLING_DATE,
                                             format = "%m/%d/%Y")) %>%
    dplyr::select(DateTime, depth, temp, DOW)
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}


#these take hourly measurements - keeping the noon measurements to
#downsample to daily
parse_Cass_lake_emperature_Logger_Database_2008_to_present <- function(inind, outind) {

  raw <- feather::read_feather(sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml'))
  outfile <- as_data_file(outind)


  clean <- raw %>%
    mutate(temp = fahrenheit_to_celsius(WaterTemp),
                          time = strftime(Time, format="%H:%M"),
                          DateTime = as.Date(Date, format = "%m/%d/%y"),
                          DOW = "04003000",
           timezone = 'CST/CDT') %>%
    dplyr::select(DateTime, time, timezone, depth, temp, DOW)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#Lake of the Woods
# no time data
parse_LotW_WQ_Gretchen_H <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- readxl::read_excel(infile)
  clean <- raw %>% filter(!grepl(pattern = "Dates in Red", x = notes) & !is.na(temp.units) & !is.na(temperature)) %>%
    mutate(DOW = gsub(pattern = "-", replacement = "", x = DOW),
           depth = convert_ft_to_m(depth),
           temp = ifelse(temp.units == "F", yes = fahrenheit_to_celsius(temperature),
                         no = temperature)) %>% rename(DateTime = Date) %>%
    dplyr::select(DateTime, temp, depth, DOW) %>% mutate(DateTime = as.Date(DateTime))
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_LOTW2018 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- read.csv(infile)
  clean <- raw %>% filter(QCflag == 0) %>%
    mutate(depth = Depth,
           temp = fahrenheit_to_celsius(Temp),
           DateTime = as.Date(Time, format = '%m/%d/%Y'),
           DOW = as.character(DOW),
           time = as.POSIXct(Time, format = '%m/%d/%Y %H:%M'),
           timezone = 'GMT-5') %>%
    mutate(time = as.character(format(time, format = '%H:%M'))) %>%
    dplyr::select(DateTime, time, timezone, depth, temp, DOW)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#mille lacs
# no time data
parse_ML_observed_temperatures <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- data.table::fread(infile)
  clean <- raw %>% mutate(temp = fahrenheit_to_celsius(temp.f),
                          depth = convert_ft_to_m(depth.ft),
                          DateTime = as.Date(Date, format = "%m/%d/%Y"),
                          DOW = "48000200") %>% dplyr::select(DateTime, temp, depth, DOW)
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

#rainy lake sand bay files
parse_Sand_Bay_all_2013 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  all_data <- tibble()
  nums <- 0:9
  skip = 2
  cols <- c("num", "time", "temp", paste0("V", 5:9))
  #the files aren't all quite the same...
  for(sheet in paste("Sand_Bay", nums, sep = "_")) {
    raw_sheet <- readxl::read_excel(infile, sheet = sheet, skip = skip,
                                    col_names = cols)
    depth_val <- 10 - as.numeric(stringr::str_sub(sheet, -1,-1))
    sheet_bind <- raw_sheet %>%
      dplyr::select(time, temp) %>%
      mutate(depth = depth_val)
    all_data <- bind_rows(all_data, sheet_bind)
  }
  all_data_clean <- all_data %>%
    mutate(DateTime = as.Date(time),
           time = substr(time, 12,16),
           temp = fahrenheit_to_celsius(temp),
           DOW = "69069400",
           timezone = 'CST/CDT') %>%
    #filter(grepl(pattern = "12:0", x = hrmin)) %>%
    dplyr::select(DateTime, time, timezone, depth, temp, DOW) %>%
    arrange(DOW, DateTime)

  saveRDS(object = all_data_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Sand_Bay_All_2016 <- parse_Sand_Bay_all_2015 <- parse_Sand_Bay_all_2014 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  sheet = "All"
  if(grepl(pattern = "2016", x = infile)){
    sheet = "Sand_Bay_All"
  }
  raw_sheet <- readxl::read_excel(infile, sheet = sheet)
  names(raw_sheet)[1] <- "DateTime"
  clean_sheet <- raw_sheet %>%
    dplyr::select(contains("Sand Bay"), contains("Date")) %>%
    tidyr::gather(key = depth, value = "temp", -DateTime) %>%
    #0 sensor is at the bottom, 10 at top
    mutate(depth = 10 - as.numeric(stringr::str_sub(depth, -1,-1))) %>%
    #filter(lubridate::hour(DateTime) == 15) %>%
    mutate(time = substr(DateTime, 12,16),
           timezone = 'GMT-5') %>%
    mutate(DateTime = as.Date(DateTime),
           temp = fahrenheit_to_celsius(temp),
           DOW = "69069400") %>%
    dplyr::select(DateTime, time, timezone, depth, temp)

  saveRDS(object = clean_sheet, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_LeechMain2018 <-
  parse_LeechWalkerBay2018 <-
  parse_MilleLacs2018 <-
  parse_Winnie2018 <-
  parse_Rainy2018 <-
  parse_Kab2018 <- function(inind, outind) {

    infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- read.csv(infile)

  clean <- raw %>% filter(QCflag == 0) %>%
    mutate(depth = Depth,
           temp = fahrenheit_to_celsius(Temp),
           DateTime = as.Date(Time, format = '%m/%d/%Y'),
           DOW = as.character(DOW),
           time = as.POSIXct(Time, format = '%m/%d/%Y %H:%M'),
           timezone = 'GMT-5') %>%
    mutate(time = as.character(format(time, format = '%H:%M'))) %>%
    dplyr::select(DateTime, time, timezone, depth, temp, DOW)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
  }

parse_South_Center_DO_2018_09_11_All <-
  parse_Greenwood_DO_2018_09_14_All <-
  parse_Carlos_DO_2018_11_05_All <- function(inind, outind){

    infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
    outfile <- as_data_file(outind)
    raw <- read.csv(infile)

    clean <- raw %>%
      mutate(depth = -Depth_m,
             Date_Time = as.POSIXct(Date_Time, format = '%Y-%m-%d %H:%M:%S'),
             DOW = as.character(DOWLKNUM),
             time = as.character(format(Date_Time, format = '%H:%M')),
             DateTime = as.Date(Date_Time),
             timezone = 'GMT-5',
             time = as.character(format(time, format = '%H:%M'))) %>%
      dplyr::select(DateTime, time, timezone, depth, temp = Water_Temp_C, DOW)

    saveRDS(object = clean, file = outfile)
    sc_indicate(ind_file = outind, data_file = outfile)

}


