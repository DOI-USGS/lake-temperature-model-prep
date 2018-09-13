# parse Lake Winnie files

parse_winnie_files <- function(inind, outind) {

  #x 2009 and 2010 have similar structure, but tab "Temp data" in 2009
  #x 2011, 2012 has seperate tabs for each depth with number and "ft" for depth
  #x 2013 5 ft is 05_ft rest are just numbers to indicate depth
  #x 2014 similar to 2009/2010 but tab "Data" and date and time in seperate columns
  #x 2015 similar to 2014, but tab "Winnie temp logger -2015"
  #x 2017 similar to 2014/2015, but tab "Winnie temp_-_2017"

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  ###################
  # 2009, 2010, 2017 data
  ###################

  if (grepl('2010|2009|2017', infile)) {
    # read in data

    if (grepl('2010', infile)) {
      raw_dat <- readxl::read_excel(infile, sheet = 'All profiles', skip = 1)

      # extract depths
      rel_cols <- names(readxl::read_excel(infile, sheet = 'All profiles'))
      depths <- grep('ft', rel_cols, value = TRUE)
      depths <- gsub("(.+Winnie\\s)(\\d+)(\\s*ft.*)", "\\2", depths)

    } else if (grepl('2009', infile)) {
      raw_dat <- readxl::read_excel(infile, sheet = 'Temp data', skip = 1)

      # extract depths
      rel_cols <- names(readxl::read_excel(infile, sheet = 'Temp data'))
      depths <- grep('ft', rel_cols, value = TRUE)
      depths <- gsub("(^.*\\s)(\\d{1,}ft)(.*)", "\\2", depths)
      depths <- gsub('ft', '', depths)

    } else if (grepl('2017', infile)){
      raw_dat <- readxl::read_excel(infile, sheet = 'Winnie temp_-_2017', skip = 1)
      rel_cols <- names(readxl::read_excel(infile, sheet = 'Winnie temp_-_2017'))

      depths <- grep('ft|feet', rel_cols, value = TRUE)
      depths <- gsub("(^.*Winnie\\s)(\\d{1,})(\\s*f.*)", "\\2", depths)
    }


  time_cols <- grep('Time', names(raw_dat))
  temp_cols <- grep('Temp', names(raw_dat))

  cleaned_dat <- as.data.frame(matrix(ncol = 3, nrow = 0))
  names(cleaned_dat) <- c('DateTime', 'temp', 'depth')

  # parse out different depths
  for (i in 1:length(depths)) {
    temp_dat <- select(raw_dat, DateTime = time_cols[i], temp = temp_cols[i]) %>%
      mutate(depth = as.numeric(depths[i]))

    cleaned_dat <- bind_rows(cleaned_dat, temp_dat)
  }

  # units
  cleaned_dat <- cleaned_dat %>%
    mutate(depth = depth/3.28,
           temp = fahrenheit_to_celsius(temp),
           DOW = '11014700',
           time = strftime(DateTime, format="%H:%M"),
           DateTime = as.Date(DateTime),
           timezone = 'GMT-6') %>%
    #filter(time == '12:00:00') %>% # subsample hourly measures to noon samples
    select(DateTime, time, timezone, depth, temp, DOW) %>%
    arrange(DateTime)

  }

  ###################
  # 2014, 2015 data
  ###################

  if (grepl('2014|2015', infile)) {

    if (grepl('2014', infile)) {
      raw_dat <- readxl::read_excel(infile, sheet = 'Data', skip = 1)
      rel_cols <- names(readxl::read_excel(infile, sheet = 'Data'))
      depths <- grep('ft', rel_cols, value = TRUE)
      depths <- gsub("(^.*\\s)(\\d{1,}\\s*ft)(.*)", "\\2", depths)
      depths <- gsub('ft', '', depths)

      # find index of temperature columns
      temp_cols <- grep('^.{1}F', names(raw_dat))

    } else if (grepl('2015', infile)) {
      raw_dat <- readxl::read_excel(infile, sheet = 'Winnie temp logger -2015', skip = 1)
      rel_cols <- names(readxl::read_excel(infile, sheet = 'Winnie temp logger -2015'))

      depths <- grep('ft|feet', rel_cols, value = TRUE)
      depths <- gsub("(^.*Winnie\\s)(\\d{1,})(\\s*f.*)", "\\2", depths)

      # find index of temperature columns
      temp_cols <- grep('Temp,', names(raw_dat))


    }

  # find columns of interest
  time_cols <- grep('GMT|Time', names(raw_dat))
  date_cols <- grep('Date', names(raw_dat))


  cleaned_dat <- as.data.frame(matrix(ncol = 4, nrow = 0))
  names(cleaned_dat) <- c('DateTime', 'Time', 'temp', 'depth')

  # parse out different depths
  for (i in 1:length(depths)) {
    temp_dat <- select(raw_dat, DateTime = date_cols[i], Time = time_cols[i], temp = temp_cols[i]) %>%
      mutate(depth = as.numeric(depths[i]))

    cleaned_dat <- bind_rows(cleaned_dat, temp_dat)
  }

  # units
  cleaned_dat <- cleaned_dat %>%
    mutate(depth = depth/3.28,
           temp = fahrenheit_to_celsius(temp),
           DOW = '11014700',
           time = strftime(Time, format="%H:%M"),
           DateTime = as.Date(DateTime),
           timezone = 'GMT-6') %>%
    #filter(Time %in% '12:00:00') %>% # subsample hourly measures to noon samples
    select(DateTime, time, timezone, depth, temp, DOW) %>%
    arrange(DateTime)

  }

  ###################
  # 2011, 2012 data
  ###################

  if (grepl('2011|2012', infile)) {
    data_sheets <- readxl::excel_sheets(infile)

    if (length(grep('graph', data_sheets, ignore.case = T)) > 0) {
      data_sheets <- data_sheets[-grep('graph', data_sheets, ignore.case = T)]
    }

    # define depths
    depths <- gsub("\\d{4}", "", data_sheets)
    depths <- gsub("(^\\D*)([[:digit:]]{1,2})(\\s*.*)", "\\2", depths)

    cleaned_dat <- as.data.frame(matrix(ncol = 4, nrow = 0))
    names(cleaned_dat) <- c('DateTime', 'time', 'temp', 'depth')

    for (i in 1:length(data_sheets)) {

      raw_dat <- readxl::read_excel(infile, sheet = data_sheets[i], skip = 1)

      time_cols <- grep('Time', names(raw_dat))
      temp_cols <- grep('Temp', names(raw_dat))

      temp_dat <- select(raw_dat, DateTime = time_cols, temp = temp_cols) %>%
        mutate(depth = as.numeric(depths[i]),
               time = strftime(DateTime, format="%H:%M", tz = 'UTC'),
               timezone = 'GMT-6')

      cleaned_dat <- bind_rows(cleaned_dat, temp_dat)
    }

    # units
    cleaned_dat <- cleaned_dat %>%
      mutate(depth = depth/3.28,
             temp = fahrenheit_to_celsius(temp),
             DOW = '11014700',
             DateTime = as.Date(DateTime)) %>%
      #filter(time == '13') %>% # subsample hourly measures to noon samples
      select(DateTime, time, timezone, depth, temp, DOW) %>%
      arrange(DateTime)


    }

  ###################
  # 2013 data
  ###################

  if (grepl('2013', infile)) {
  data_sheets <- readxl::excel_sheets(infile)

  if (length(grep('graph', data_sheets, ignore.case = T)) > 0) {
    data_sheets <- data_sheets[-grep('graph', data_sheets, ignore.case = T)]
  }

  # define depths
  depths <- gsub("\\d{4}", "", data_sheets)
  depths <- gsub("(^\\D*)([[:digit:]]{1,2})(\\s*.*)", "\\2", depths)

  cleaned_dat <- as.data.frame(matrix(ncol = 5, nrow = 0))
  names(cleaned_dat) <- c('DateTime', 'time', 'am_pm', 'temp', 'depth')

  for (i in 1:length(data_sheets)) {

    raw_dat <- readxl::read_excel(infile, sheet = data_sheets[i], skip = 2, col_names = F)


    temp_dat <- select(raw_dat, DateTime = 2, time = 3, am_pm = 4, temp = 5) %>%
      mutate(depth = as.numeric(depths[i]),
             hour = strftime(time, format="%H", tz = 'UTC'),
             minute = strftime(time, format="%M", tz = 'UTC'),
             timezone = 'GMT-6') %>%
      mutate(hour = ifelse(am_pm == 'PM'&as.numeric(hour) != 12, as.character(as.numeric(hour) + 12), hour)) %>%
      mutate(time = paste0(hour, ':', minute))

    cleaned_dat <- bind_rows(cleaned_dat, temp_dat)
  }

  # units
  cleaned_dat <- cleaned_dat %>%
    mutate(depth = depth/3.28,
           temp = fahrenheit_to_celsius(temp),
           DOW = '11014700',
           DateTime = as.Date(DateTime),
           timezone = 'GMT-6') %>%
    #filter(time == '12:00:00' & am_pm == 'PM') %>% # subsample hourly measures to noon samples
    select(DateTime, time, timezone, depth, temp, DOW) %>%
    arrange(DateTime)

  }

  saveRDS(object = cleaned_dat, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
