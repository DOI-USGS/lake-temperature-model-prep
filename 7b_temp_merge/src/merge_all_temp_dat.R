merge_temp_data <- function(outind, wqp_ind, coop_ind) {

  outfile <- as_data_file(outind)

  wqp_in <- sc_retrieve(wqp_ind)
  wqp_dat <- feather::read_feather(wqp_in)

  coop_in <- sc_retrieve(coop_ind)
  coop_dat <- feather::read_feather(coop_in)

  total_obs <- nrow(wqp_dat) + nrow(coop_dat)

  # bind data together
  # get rid of egregious values by jan/feb and july/aug
  # get rid of negative depths
  # get rid of data before 1979
  # before doing anything else

  f_all_dat <- dplyr::select(wqp_dat, date = Date, time, timezone, depth, temp = wtemp, nhdhr_id = id, source_id = MonitoringLocationIdentifier, source_site_id = MonitoringLocationIdentifier) %>%
    mutate(source = 'wqp') %>%
    bind_rows(dplyr::select(coop_dat, date = DateTime, time,
                            timezone, depth, temp, nhdhr_id = site_id, source_id = state_id,
                            source_site_id = site, source)) %>%
    mutate(month = lubridate::month(date)) %>%
    filter(!(month %in% c(1, 2) & temp > 10)) %>%
    filter(!(month %in% c(7, 8) & depth < 0.5 & temp < 10)) %>%
    mutate(timezone = ifelse(is.na(time), NA, timezone))

  recent_f_dat <- filter(f_all_dat, date >= as.Date('1979-01-01'))

  cat(total_obs - nrow(f_all_dat), 'temperature observations were flagged as egregious (>10 deg C in Jan/Feb or <10 deg C at surface in July/Aug) or had negative depths and were removed.')
  cat('\n', nrow(f_all_dat) - nrow(recent_f_dat), 'historical (pre-1979) temperature records were removed because they were outside of the modeling time frame.')


  # merge and remove duplicate observations
  # first check for duplicates from the same sources
  # depth and temp

  no_dupes_dat <-  recent_f_dat %>%
    mutate(depth = round(depth, 2),
           depth1 = round(depth, 1),
           temp = round(temp, 2),
           temp1 = round(temp, 1)) %>%
    distinct()


  # remove duplicates again
  # now defined as the same temperature observation
  # for any given lake-site-date-depth
  # (temp and depth rounded to 1 decimal for matching purposes)
  # this will take the first source metadata with it
  all_dat_distinct <- no_dupes_dat %>%
    distinct(nhdhr_id, source_site_id, date, time, depth1, temp1, .keep_all = TRUE) %>%
    dplyr::select(-depth1, -temp1)

  cat('\n', nrow(recent_f_dat) - nrow(all_dat_distinct), "duplicated lake-site-date-depth-temp observations were removed. The first record's metadata (e.g., source) were preserved.")

  # note - could still have lake/date/depth duplicates here
  # the risk of removing time from the above distinct() is that if you have
  # multiple observations per day, you could inadvertently lose obs that are distinct
  # in time but happen to have the same values (e.g., noon and 1pm surface samples that measure
  # the same temperature). The risk of not removing them is that you have multiple data sources
  # repeating the same data - but may not have a time stamp, for example. Will keep for now, sort out
  # in next step where we resolve time.

  feather::write_feather(all_dat_distinct_noout, outfile)
  gd_put(outind, outfile)

}

reduce_temp_data <- function(outind, inind) {

  outfile <- as_data_file(outind)

  all_in <- sc_retrieve(inind)
  all_dat <- feather::read_feather(all_in)

  # handle data with no times reported - check assumption that this subset does not
  # have repeated values

  dat_notimes <- filter(all_dat, is.na(time)) %>%
    distinct(nhdhr_id, date, depth, temp, .keep_all = TRUE)

  # if we have multiple observations per nhdhr_id/site/date/depth with no time stamp,
  # we assume these were
  # taken at different times and choose a single time
  # take the middle value -- e.g., if there are 3 obs, take 2nd - best chance to capture mid-day
  dat_notimes_timeresolved <- filter(dat_notimes, !is.na(source_site_id)) %>%
    group_by(nhdhr_id, date, depth, source_site_id) %>%
    mutate(n = n()) %>%
    summarize(temp = temp[floor(mean(n)/2) + 1]) %>%
    bind_rows(filter(dat_notimes, is.na(source_site_id))) #bind back together with data with no siteids

  # the reasons for the rest of the repeated observations are unknown

  #######################
  # get data with time
  # create a variable that represents difference from noon in hours
  # assume all UTC timezones are GMT-5

  # find local times closest to noon
  # adjust UTC to be central time by subtracting 7 hours instead of 12

  # FIX THIS
  # more time zones
  # need to account for pos or neg diff from noon
  dat_times <- filter(all_dat, !is.na(time)) %>%
    mutate(time_hm = lubridate::hm(time)) %>%
    mutate(hours_diff_noon = case_when(timezone %in% 'UTC' ~ abs(time_hm$hour - 7) + (time_hm$minute/60),
                                     TRUE ~ abs(time_hm$hour - 12) - (time_hm$minute/60)))

  # group by lake, siteid, date, depth and take the time closest to noon
  dat_singletimes <- dat_times %>%
    group_by(nhdhr_id, source_site_id,  date, depth) %>%
    summarize(temp = temp[which.min(hours_diff_noon)],
              time = time[which.min(hours_diff_noon)],
              ntimeobs = n()) %>%
    distinct(nhdhr_id, date, depth, temp, .keep_all = TRUE) %>%
    ungroup() %>%
    arrange(nhdhr_id, date, depth)

  cat('There were', length(which(dat_singletimes$ntimeobs > 1)),
      "unique lake-site-date-depths with repeated observations where the observation with the timestamp closest to noon was retained.")


  # now we bring data back together and handle sites that may not have had timestamps
  # this first distinct has the potential to drop observations from multiple sites that have
  # the same depth/temp/day -- however, I think the risk of having duplicate data is higher than
  # dropping values that are the same which would be averaged later
  all_dat_singletimes <- bind_rows(dplyr::select(dat_notimes_timeresolved, -time, -timezone),
                                   dplyr::select(dat_singletimes, -time, -ntimeobs)) %>%
    distinct(nhdhr_id, date, depth, temp, .keep_all = TRUE) %>%
    group_by(nhdhr_id, date, depth) %>%
    summarize(temp = mean(temp)) %>%
    ungroup()

  all_dat_singletimes_recent <- filter(all_dat_singletimes, date >= as.Date('1979-01-01'))

  cat('There were', nrow(all_dat_singletimes) - nrow(all_dat_singletimes_recent),
      "historical (prior to 1979) observations that were dropped.")

  feather::write_feather(all_dat_singletimes_recent, outfile)
  gd_put(outind, outfile)

}


