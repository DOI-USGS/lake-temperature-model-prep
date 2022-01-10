parse_upper_lower_redlake_files <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')

  outfile <- as_data_file(outind)

  # read in data
  raw_dat <- readxl::read_excel(infile)

  # select columns, rename units, rename columns
  date_col <- vars_select(names(raw_dat), contains('date', ignore.case = TRUE))

  cleaned_dat <- mutate(raw_dat, time = strftime(raw_dat[[date_col]], format = "%H:%M"))

  # which time stamp has the most observations? Use that one
  #n_time <- group_by(cleaned_dat, time) %>%
    #filter(grepl('^07:', time)) %>% # 7th hour which translates to noon
    #summarize(n = n()) %>%
    #arrange(-n)

  #time_keep <- n_time$time[1]

  cleaned_dat <- cleaned_dat %>%
    #filter(time == time_keep) %>%
    mutate(DateTime = as.Date(.[[date_col]]),
           depth = ifelse(depth.units %in% 'f', convert_ft_to_m(depth), depth),
           temp = ifelse(temp.units %in% 'C', temperature, fahrenheit_to_celsius(temperature)),
           timezone = 'GMT-5') %>%
    dplyr::select(DateTime, time, timezone, depth, temp, DOW) %>%
    filter(!is.na(depth), !is.na(temp)) %>%
    arrange(DateTime)

  # need special accomdation for 2015, which does not have times
  if (grepl('2015', infile)) {
    cleaned_dat <- raw_dat %>%
      mutate(DateTime = as.Date(.[[date_col]]),
             depth = ifelse(depth.units %in% 'f', convert_ft_to_m(depth), depth),
             temp = ifelse(temp.units %in% 'C', temperature, fahrenheit_to_celsius(temperature))) %>%
      dplyr::select(DateTime, depth, temp, DOW) %>%
      group_by(DateTime) %>%
      summarize(depth = first(depth),
                    temp = first(temp),
                    DOW = first(DOW))
  }

  saveRDS(object = cleaned_dat, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}
