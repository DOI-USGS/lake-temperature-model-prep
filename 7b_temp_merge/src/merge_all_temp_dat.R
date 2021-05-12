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

  f_all_dat <- dplyr::select(wqp_dat, date = Date, time, timezone, depth, temp = wtemp, site_id = id, source_id = MonitoringLocationIdentifier, source_site_id = MonitoringLocationIdentifier) %>%
    mutate(source = sprintf('wqp_%s', source_id)) %>%
    bind_rows(dplyr::select(coop_dat, date = DateTime, time,
                            timezone, depth, temp, site_id, source_id = state_id,
                            source_site_id = site, source)) %>%
    mutate(month = lubridate::month(date)) %>%
    filter(!(month %in% c(1, 2) & temp > 10)) %>%
    filter(!(month %in% c(7, 8) & depth < 0.5 & temp < 10)) %>%
    mutate(timezone = ifelse(is.na(time), NA, timezone))

  recent_f_dat <- filter(f_all_dat, date >= as.Date('1979-01-01'))

  recent_f_dat_noneg <- filter(recent_f_dat, depth >= 0)

  cat(total_obs - nrow(f_all_dat), 'temperature observations were flagged as egregious (>10 deg C in Jan/Feb or <10 deg C at surface in July/Aug) and removed.')
  cat('\n', nrow(f_all_dat) - nrow(recent_f_dat), 'historical (pre-1979) temperature records were removed because they were outside of the modeling time frame.')
  cat('\n', nrow(recent_f_dat) - nrow(recent_f_dat_noneg), 'temperature records were removed because they had negative depth values.')


  # merge and remove duplicate observations
  # first check for duplicates from the same sources and sites
  # depth and temp

  no_dupes_dat <-  recent_f_dat_noneg %>%
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
    distinct(site_id, source_site_id, date, time, depth1, temp1, .keep_all = TRUE) %>%
    dplyr::select(-depth1, -temp1)

  cat('\n', nrow(recent_f_dat_noneg) - nrow(all_dat_distinct), "duplicated lake-site-date-depth-temp observations were removed. The first record's metadata (e.g., source) were preserved.")

  # note - could still have lake/date/depth duplicates here
  # the risk of removing time from the above distinct() is that if you have
  # multiple observations per day, you could inadvertently lose obs that are distinct
  # in time but happen to have the same values (e.g., noon and 1pm surface samples that measure
  # the same temperature). The risk of not removing them is that you have multiple data sources
  # repeating the same data - but may not have a time stamp, for example. Will keep for now, sort out
  # in next step where we resolve time.

  feather::write_feather(all_dat_distinct, outfile)
  gd_put(outind, outfile)

}

reduce_temp_data <- function(outind, inind) {

  outfile <- as_data_file(outind)

  all_in <- sc_retrieve(inind)
  all_dat <- feather::read_feather(all_in)

  # find data with multiple obs per lake/site/date/time/depth

  # true single values, with only a single lake-date-depth
  # we can leave these values alone
  true_singles <- group_by(all_dat, site_id, date, depth) %>%
    mutate(n_coarse = n()) %>%
    filter(n_coarse == 1) %>%
    ungroup()

  # all other values
  multiples <- group_by(all_dat, site_id, date, depth) %>%
    mutate(n_coarse = n()) %>%
    filter(n_coarse > 1) %>%
    ungroup() %>%
    # count number of values per lake-site-source-date-depth
    group_by(site_id, source, source_site_id, date, depth) %>%
    mutate(n_fine = n()) %>%
    ungroup()

  ###########################
  # sort out multiple values
  ###########################

  # first, we assume that all lake-source-site-date-depth multiples
  # with time stamps are high-frequency measures (e.g., >1/day)
  # without time stamps, the observations are either multiple sites or times

  # first are data that don't have multiple lake-source-site-date-depth vals
  other_multiples <- filter(multiples, n_fine == 1)

  # then are what we expect to be time multiples
  time_multiples <- filter(multiples, n_fine > 1) %>%
    group_by(site_id, source, source_site_id, date, depth) %>%
    mutate(has_time = all(!is.na(time))) %>%
    filter(has_time) %>%
    ungroup()

  # then are what we expect to be time or space multiples
  unknown_multiples <- filter(multiples, n_fine > 1) %>%
    group_by(site_id, source, source_site_id, date, depth) %>%
    mutate(has_time = all(!is.na(time))) %>%
    filter(!has_time) %>%
    ungroup()

  ##########################################################
  ############## sort out "time" data with time stamps
  ###########################################################

  # Right now, we assume all instances with multiple values per lake-source-site-date-depth
  # are repeated in time. However, there may be NA for site where actually there are >1
  # sites measured and no site reported. But maybe this is the best/only way?

  # create a variable that represents difference from noon in hours

  # find local times closest to noon
  # adjust UTC & GMT to be central time (CDT) before calulcation of diff from noon

  # first, convert UTC and GMT to CST/CDT
  utcgmt <- filter(time_multiples, timezone %in% c('UTC', 'GMT')) %>%
    mutate(date_time = paste(date, time, sep = ' ')) %>%
    mutate(date_time = as.POSIXct(date_time, tz = 'UTC', format = '%Y-%m-%d %H:%M')) %>%
    mutate(c_date_time = lubridate::with_tz(date_time, tzone = 'America/Chicago')) %>%
    mutate(date = lubridate::as_date(c_date_time),
           time = format(c_date_time, '%H:%M'),
           timezone = lubridate::tz(c_date_time)) %>%
    dplyr::select(-date_time, -c_date_time)

  # bind data back together and calculate time from noon
  # this now assumed all data are in a time zone that are local to the lake
  dat_times_c <- filter(time_multiples, !timezone %in% c('UTC', 'GMT')) %>%
    bind_rows(utcgmt) %>%
    mutate(time_hm = lubridate::hm(time)) %>%
    mutate(hours_minus_noon = time_hm$hour - 12,
           minutes_decimal = time_hm$minute/60) %>%
    mutate(hours_diff_noon = abs(hours_minus_noon + minutes_decimal))

  # group by lake, siteid, date, depth and take the time closest to noon
  # if site is missing, we assume a single site for that lake-date
  resolved_time_multiples <- dat_times_c %>%
    group_by(site_id, source, source_site_id, date, depth) %>%
    dplyr::summarize(temp = temp[which.min(hours_diff_noon)],
                     time = time[which.min(hours_diff_noon)],
                     source_id = source_id[which.min(hours_diff_noon)])

  cat(nrow(time_multiples) - nrow(resolved_time_multiples), "high frequency (>1/day) observations were dropped. Observations with time closest to noon (local time) were retained when possible.")

  ##########################################################
  ############## sort out "other" instances with multiple obs per lake-source-site-date-depth
  ###########################################################
  # these data could be multiple sites (where site is not reported) or multiple times (where time is not reported)
  # in any case, we try to grab the most representative sample from that day
  # we don't have labels to make sure we're grabbing the same profile or same site
  # for each lake-source-site-date-depth, we find the index of the median temperature value
  # for that lake-source-site-date, we find the index that is most often the median value (the mode),
  # and use that index to subset each lake-source-site-date-depth in hopes of grabbing the same time or site
  # across depths


  getmode <- function(v) {
    uniqv <- unique(v)
    uniqv[which.max(tabulate(match(v, uniqv)))]
  }

  resolved_unknown_multiples <- unknown_multiples %>%
    group_by(site_id, source, source_site_id, date, depth) %>%
    mutate(median_index = which.min(round(abs(temp - median(temp)), 2))) %>%
    group_by(site_id, source, source_site_id, date) %>%
    # make sure the same record (by order) is kept for each lake-source-date
    # in an attempt to grab the same time or site the  across depths
    # do this by assigning the keep to the index that appears
    # most often as the closest to median value
    mutate(which_keep = getmode(median_index)) %>%
    ungroup() %>%
    group_by(site_id, source, source_site_id, date, depth) %>%
    dplyr::summarize(temp = temp[first(which_keep)],
                     source_id = first(source_id)) %>%
    ungroup() %>%
    filter(!is.na(temp))

  # some NA values emerge because which_keep could be > than the number of observations per
  # depth. This is the sacrafice for trying to take same index of observations
  # Latest run, this was 88 observations

  cat(nrow(unknown_multiples) - nrow(resolved_unknown_multiples), "repeated observations were dropped. It was unknown if the obs. were repeated in time or space. The most representative values at each date were chosen.")

  ####################################################
  ## Now all lake-source-site-date-depth multiples are resolved
  ## Bind everything back together to deal with lake-date-depth multiples
  ##########################################################

  reduced_multiples <- bind_rows(other_multiples, resolved_time_multiples, resolved_unknown_multiples) %>%
    group_by(site_id, date, depth) %>%
    mutate(n = n()) %>%
    filter(n > 1) %>%
    ungroup()

  singles <- bind_rows(other_multiples, resolved_time_multiples, resolved_unknown_multiples) %>%
    group_by(site_id, date, depth) %>%
    filter(n() == 1) %>%
    ungroup() %>%
    bind_rows(true_singles)

  # we know that remaining multiples are because of multiples source-sites per lake-date-depth
  # we can drop the time stamp and look for duplicates now

  # first, create a table that shows the "quality" of each lake-source-site
  # if this site is present in any lake-date-depth combo, choose it
  top_source_sites <- group_by(bind_rows(reduced_multiples, singles), site_id, source, source_site_id, date) %>%
    dplyr::summarize(n_depths = n()) %>%
    group_by(site_id, source, source_site_id) %>%
    dplyr::summarize(n_dates = n(),
                     avg_n_depths = mean(n_depths)) %>%
    group_by(site_id) %>%
    dplyr::summarize(source = source[which.max(n_dates)],
                     source_site_id = source_site_id[which.max(n_dates)],
                     avg_n_depths = avg_n_depths[which.max(n_dates)],
                     n_dates = max(n_dates)) %>%
    filter(n_dates > 50 & avg_n_depths >= 5) %>%
    mutate(top_site = 1) %>%
    dplyr::select(site_id, source, source_site_id, top_site) %>%
    ungroup()

  top_daily_source_sites <- group_by(reduced_multiples, site_id, source, source_site_id, date) %>%
    dplyr::summarize(n_depths = n()) %>%
    group_by(site_id, date) %>%
    dplyr::summarize(source = source[which.max(n_depths)],
                     source_site_id = source_site_id[which.max(n_depths)]) %>%
    mutate(top_site_date = 1) %>%
    ungroup()

  # check these calcs
  # maybe leave single dat out of it
  daily_vals <- reduced_multiples %>%
    left_join(top_source_sites) %>%
    left_join(top_daily_source_sites) %>%
    # first use global best if available, then use local best if available,
    # then use first source. This takes the same source for a given date
    # across all depths if available
    mutate(keep_source_site = ifelse(top_site %in% 1 | top_site_date %in% 1, TRUE, FALSE)) %>%
    group_by(site_id, date, depth) %>%
    dplyr::summarize(temp = ifelse(all(!(keep_source_site)), first(temp), temp[keep_source_site]),
                     source = ifelse(all(!(keep_source_site)), first(source), source[keep_source_site]),
                     source_site_id = ifelse(all(!(keep_source_site)), first(source_site_id), source_site_id[keep_source_site]),
                     source_id = ifelse(all(!(keep_source_site)), first(source_id), source_id[keep_source_site])) %>%
    ungroup() %>% arrange(site_id, date, depth)

  all_dailies <- bind_rows(daily_vals, singles) %>%
    dplyr::select(site_id, date, depth, temp, source)

  cat(nrow(reduced_multiples) - nrow(daily_vals), "temperature observations were dropped due to multiples source-sites per lake-date-depth. The 'best' source-site was chosen when available.")

  cat("\nThere are", nrow(all_dailies), "temperature observations in the final dataset.")

  feather::write_feather(all_dailies, outfile)
  gd_put(outind, outfile)

}

reduce_reservoir_data <- function(outind, drb_ind, nyc_dep_ind) {
  # group by depth category
  # select value closest to noon
  drb_reservoirs_temps <- readRDS(sc_retrieve(drb_ind)) %>%
    mutate(dateTime_est = as.POSIXct(dateTime, tz = 'America/New_York'),
           time = format(dateTime, '%H:%M'),
           time_hm = lubridate::hm(time),
           hours_minus_noon = time_hm$hour - 12,
           minutes_decimal = time_hm$minute/60,
           hours_diff_noon = abs(hours_minus_noon + minutes_decimal)) %>%
    arrange(site_id, date, depth_category)

  nyc_dep_reservoirs_temps <- readRDS(sc_retrieve(nyc_dep_ind))

  # Selecting the unique dates associated with each reservoir.
  cannonsville_dates <- unique(nyc_dep_reservoirs_temps$date[nyc_dep_reservoirs_temps$site_id == 'nhdhr_120022743'])
  pepacton_dates <- unique(nyc_dep_reservoirs_temps$date[nyc_dep_reservoirs_temps$site_id == 'nhdhr_151957878'])

  # filter out site-dates that are already accounted for in the NYC DEP data
  daily_drb_dat <- drb_reservoirs_temps %>%
    filter(!(site_id %in% 'nhdhr_120022743' & date %in% cannonsville_dates) &
             !(site_id %in% 'nhdhr_151957878' & date %in% pepacton_dates)) %>%
    group_by(site_id, date, depth_category) %>%
    slice_min(hours_diff_noon) %>%
    ungroup() %>%
    dplyr::select(site_id, source_id, date, depth, temp)

  combine_dat <- bind_rows('nwis' = daily_drb_dat,
                           'nyc_dep' = nyc_dep_reservoirs_temps, .id = 'source') %>%
    dplyr::select(site_id, date, source_id, source, depth, temp)

  saveRDS(combine_dat, as_data_file(outind))
  gd_put(outind)

}

