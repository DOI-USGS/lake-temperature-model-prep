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
    distinct(nhdhr_id, source_site_id, date, time, depth1, temp1, .keep_all = TRUE) %>%
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
  true_singles <- group_by(all_dat, nhdhr_id, date, depth) %>%
    mutate(n_coarse = n()) %>%
    filter(n_coarse == 1) %>%
    ungroup()

  # all other values
  multiples <- group_by(all_dat, nhdhr_id, date, depth) %>%
    mutate(n_coarse = n()) %>%
    filter(n_coarse > 1) %>%
    ungroup()

  ###########################
  # sort out multiple values
  ###########################

  # first, if a lake-date-depth has multiple sources, choose the
  # "best" source. This best source is based on which sources
  # have the most days of observation across the entire dataset.
  # If the best overall site is not available, then the source with the
  # most data on that day is chosen.

  # create "global" best sources to choose from all multiples data
  # always choose this source when available
  best_sources <- all_dat %>%
    group_by(nhdhr_id, source) %>%
    summarize(n_days = length(unique(date))) %>%
    group_by(nhdhr_id) %>%
    summarize(overall_best_source = source[which.max(n_days)])

  # create a back-up choice for this dataset specifically
  # if global choice not available, choose the source with the most
  # data on that date
  local_sources <- multiples %>%
    group_by(nhdhr_id, source, date) %>%
    summarize(n_per_sourcedate = n()) %>%
    group_by(nhdhr_id, date) %>%
    summarize(local_best_source = source[which.max(n_per_sourcedate)])


  # only want a single source per date
  # this avoids: e.g., two sites from two different collectors
  # one has depths to 10m, one has depths to 20m.
  # 0-10 might be from source 1, 10-20 might be from source 2
  multiples_single_source <- multiples %>%
    left_join(best_sources) %>%
    left_join(local_sources) %>%
    mutate(use_source = source %in% overall_best_source,
           use_source2 = source %in% local_best_source) %>%
    group_by(nhdhr_id, date) %>%
    # first use global best if available, then use local best if available,
    # then use first source. This takes the same source for a given date
    # across all depths if available
    mutate(keep = ifelse(any(use_source), source[use_source],
                         ifelse(any(use_source2), source[use_source2], first(source)))) %>%
    ungroup() %>%
    filter(source == keep)

  cat(nrow(multiples) - nrow(multiples_single_source), "temperature observations were dropped due to multiple sources of data per lake-date. The 'best' source was kept when possible.")


  # now some of these are true single data now that multiple sources have been dropped
  resolved_multiples1 <- multiples_single_source %>%
    group_by(nhdhr_id, date, depth) %>%
    mutate(n_coarse = n()) %>%
    filter(n_coarse == 1)

  remaining_multiples <- multiples_single_source %>%
    group_by(nhdhr_id, date, depth) %>%
    mutate(n_coarse = n()) %>%
    filter(n_coarse > 1)

  # first, handle data we assume to be from multiple times at a single site
  # will have the same lake-source-site-date-depth
  # we will sort out these assumptions in more detail later on
  time_multiples <- remaining_multiples %>%
    group_by(nhdhr_id, source, source_site_id, date, depth) %>%
    mutate(n_fine = n()) %>%
    filter(n_fine > 1) %>%
    ungroup()

  # other multiples are resolved down to the source-site, but
  # have multiples across source-sites
  # this could be multiple sources with the same data, multiple sources
  # with different data, single source with multiple sites
  other_multiples <- remaining_multiples %>%
    group_by(nhdhr_id, source, source_site_id, date, depth) %>%
    mutate(n_fine = n()) %>%
    filter(n_fine == 1) %>%
    ungroup()

  # find the best sites from the remaining data
  best_sites <- bind_rows(true_singles, resolved_multiples1, remaining_multiples) %>%
    group_by(nhdhr_id, source, source_site_id) %>%
    summarize(n_site_days = length(unique(date))) %>%
    group_by(nhdhr_id, source) %>%
    summarize(overall_best_site = source_site_id[which.max(n_site_days)]) %>%
    ungroup()


  #################################
  # sort out the "other" multiples, which we assume are from multiple sites

  # pick either site with most data
  # or first reported site, depending on which is available
  # this includes sites where you could have one declared and one undeclared
  # site ID (e.g., "site 1" and "NA")

  best_local_sites <- other_multiples %>%
    group_by(nhdhr_id, source, source_site_id) %>%
    summarize(n_site_days = length(unique(date))) %>%
    group_by(nhdhr_id, source) %>%
    summarize(local_best_site = source_site_id[which.max(n_site_days)]) %>%
    ungroup()

  resolved_multiples2 <- other_multiples %>%
    left_join(best_sites) %>%
    left_join(best_local_sites) %>%
    mutate(use_site = source_site_id %in% overall_best_site,
           use_site2 = source_site_id %in% local_best_site) %>%
    group_by(nhdhr_id, date, depth) %>%
    summarize(temp = ifelse(any(use_site), temp[use_site],
                            ifelse(any(use_site2), temp[use_site2], first(temp))),
              source = first(source),
              source_site_id = ifelse(any(use_site), source_site_id[use_site],
                                      ifelse(any(use_site2), source_site_id[use_site2], first(source_site_id))),
              source_id = first(source_id))

  # insert message
  cat(nrow(other_multiples) - nrow(resolved_multiples2), "temperature records were dropped due multiple sites per lake-day-depth. The 'best' site
  was chosen when available.")


  ###################################
  # sort out "time" multiples
  # note that there may or may not be data in "times" column

  # handle data with no times reported
  # first, do we think these are multiple sites or multiple times or both?
  # first, handle data with no times + site information

  dat_notimes <- filter(time_multiples, is.na(time))
  dat_times <- filter(time_multiples, !is.na(time))

  ###### sort out "time" multiples with no time stamp ######

  # data with site information

  # if we have multiple observations per nhdhr_id/site/date/depth with no time stamp,
  # we assume these were taken at different times and choose a single time
  # take the middle value -- e.g., if there are 3 obs, take 2nd - best chance to capture mid-day
  # if this is true (multiple obs/day), the values shouldn't be that different from each other
  # filter out that day's obs if there is >3 deg C range for that lake/site/date/depth
  res1_dat_notimes <- filter(dat_notimes, !is.na(source_site_id)) %>%
    group_by(nhdhr_id, source, source_site_id, date, depth) %>%
    mutate(n = n(),
           temp_range = max(temp) - min(temp)) %>%
    filter(temp_range <= 3) %>%
    summarize(temp = temp[floor(mean(n)/2) + 1],
              source_id = first(source_id))

  # data without site information

  # if we have no site information, we assume the multiple values could
  # be from either multiple times or multiple sites

  # first, we know that data from the files that end in 'historicalfiles_manual'
  # are likely multiple site data (from the notes column in those data, there is some site
  # info but is not yet parsed)

  # take the first observation, which is the first reported "site"

  res2_dat_notimes <- filter(dat_notimes, is.na(source_site_id)) %>%
    filter(grepl('manualentry', source)) %>%
    group_by(nhdhr_id, source, date, depth) %>%
    summarize(temp = first(temp),
              source_id = first(source_id))

  # all other data - if there are > 12 obs/lake-day-depth
  # we assume these are high frequency obs and take the middle value to get closest to noon
  # also, we deduce that if depths (per date) were measured a different amount of times
  # there is a better chance for those data to be from different sites (not times)
  dat_notimes_npersource <- filter(dat_notimes, is.na(source_site_id)) %>%
    filter(!grepl('manualentry', source)) %>%
    group_by(nhdhr_id, source, date, depth) %>%
    mutate(n_per_day = n()) %>%
    group_by(nhdhr_id, source, date) %>%
    mutate(same_depth = length(unique(as.numeric(summary(as.factor(depth))))) == 1) %>%
    arrange(nhdhr_id, date, depth) %>%
    ungroup()

  # what we assume to be different sites, take first record
  res3_dat_notimes <- filter(dat_notimes_npersource, !same_depth) %>%
    group_by(nhdhr_id, date, depth) %>%
    summarize(temp = first(temp),
              source_id = first(source_id),
              source = first(source))

  # what we assume to be different times, take middle record
  res4_dat_notimes <- filter(dat_notimes_npersource, same_depth & n_per_day >=12) %>%
    group_by(nhdhr_id, date, depth) %>%
    summarize(temp = temp[floor(mean(n_fine)/2) + 1],
              source_id = first(source_id),
              source = first(source))

  # the reasons for the rest of the repeated observations are unknown
  # take the value closest to the median of that lake-source-date-depth
  # if it's time, hopefully this captures mid day
  # if it's space, hopefully this captures the most representative site

  getmode <- function(v) {
    uniqv <- unique(v)
    uniqv[which.max(tabulate(match(v, uniqv)))]
  }

  res5_dat_notimes <- filter(dat_notimes_npersource, same_depth & n_per_day < 12) %>%
    group_by(nhdhr_id, date, depth) %>%
    mutate(closest_med = which.min(round(abs(temp - median(temp)), 2))) %>%
    group_by(nhdhr_id, date) %>%
    # make sure the same record (by order) is kept for each lake-source-date
    # in an attempt to keep the time or site the same across depths
    # do this by assigning the keep to the observation (in order) that appears
    # most often as the closest to median value
    mutate(which_keep = getmode(closest_med)) %>%
    ungroup() %>%
    group_by(nhdhr_id, date, depth) %>%
    summarize(temp = temp[first(which_keep)],
              source_id = first(source_id),
              source = first(source_id))

  resolved_datnotimes <- bind_rows(res1_dat_notimes, res2_dat_notimes, res3_dat_notimes, res4_dat_notimes, res5_dat_notimes)


  ############## sort out "time" data with time stamps

  # THIS SECTION COULD PROBABLY USE MORE LOVE
  # Right now, we assume all instances with multiple values per lake-site-date-depth
  # are repeated in time. However, there may be NA for site where actually there are >1
  # sites measured and no site reported. But maybe this is the best way?

  # create a variable that represents difference from noon in hours

  # find local times closest to noon
  # adjust UTC & GMT to be central time (CDT) before calulcation of diff from noon

  # first, convert UTC and GMT to CST/CDT
  utcgmt <- filter(dat_times, timezone %in% c('UTC', 'GMT')) %>%
    mutate(date_time = paste(date, time, sep = ' ')) %>%
    mutate(date_time = as.POSIXct(date_time, tz = 'UTC', format = '%Y-%m-%d %H:%M')) %>%
    mutate(c_date_time = lubridate::with_tz(date_time, tzone = 'America/Chicago')) %>%
    mutate(date = lubridate::as_date(c_date_time),
           time = format(c_date_time, '%H:%M'),
           timezone = lubridate::tz(c_date_time)) %>%
    dplyr::select(-date_time, -c_date_time)

  # merge data back together and calculate time from noon
  # this now assumed all data are in a time zone that are local to the lake
  dat_times_c <- filter(dat_times, !timezone %in% c('UTC', 'GMT')) %>%
    bind_rows(utcgmt) %>%
    mutate(time_hm = lubridate::hm(time)) %>%
    mutate(hours_minus_noon = time_hm$hour - 12,
           minutes_decimal = time_hm$minute/60) %>%
    mutate(hours_diff_noon = abs(hours_minus_noon + minutes_decimal))

  # group by lake, siteid, date, depth and take the time closest to noon
  # if site is missing, we assume a single site for that lake-date
  dat_singletimes <- dat_times_c %>%
    group_by(nhdhr_id, source_site_id, date, depth) %>%
    summarize(temp = temp[which.min(hours_diff_noon)],
              time = time[which.min(hours_diff_noon)],
              source = source[which.min(hours_diff_noon)],
              source_id = source_id[which.min(hours_diff_noon)])

  # that was by site, there still may be multiple sites per lake
  best_local_sites <- dat_singletimes %>%
    group_by(nhdhr_id, source, source_site_id) %>%
    summarize(n_obs_site = length(unique(date))) %>%
    group_by(nhdhr_id, source) %>%
    summarize(local_best_site = source_site_id[which.max(n_obs_site)]) %>%
    ungroup()

  resolved_dattimes <- dat_singletimes %>%
    left_join(best_sites) %>%
    left_join(best_local_sites) %>%
    mutate(use_site = source_site_id %in% overall_best_site,
           use_site2 = source_site_id %in% local_best_site) %>%
    group_by(nhdhr_id, date, depth) %>%
    summarize(temp = ifelse(any(use_site), temp[use_site],
                            ifelse(any(use_site2), temp[use_site2], first(temp))),
              source = first(source),
              source_id = first(source_id))


  cat(nrow(dat_times) - (nrow(resolved_dattimes) + nrow(resolved_datnotimes)), "repeated observations were dropped. Observations with time closest to noon (local time) were retained when possible.")

  # now bring data all back together
  all_resolved <- bind_rows(true_singles, resolved_multiples1, resolved_multiples2, resolved_datnotimes, resolved_dattimes)

  # all remaining duplicates should be multiple sites, so choose best site
  # now that data is all back together, we can choose the best site per lake-source combo
  # with teh second option as the best site per lake-date combo
  best_source_site <- all_resolved %>%
    group_by(nhdhr_id, source, source_site_id) %>%
    summarize(n_obs_site = length(unique(date))) %>%
    group_by(nhdhr_id, source) %>%
    summarize(local_best_site = source_site_id[which.max(n_obs_site)]) %>%
    ungroup()

  best_date_site <- all_resolved %>%
    group_by(nhdhr_id)

  feather::write_feather(all_dat_singletimes_recent, outfile)
  gd_put(outind, outfile)

}


