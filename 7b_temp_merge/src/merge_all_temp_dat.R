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
  best_sources <- all_dat %>%
    group_by(nhdhr_id, source) %>%
    summarize(n_days = length(unique(date))) %>%
    group_by(nhdhr_id) %>%
    summarize(overall_best_source = source[which.max(n_days)])

  # reduce to a single source for each lake-date-depth
  local_sources <- multiples %>%
    group_by(nhdhr_id, source, date) %>%
    summarize(n_per_sourcedate = n()) %>%
    group_by(nhdhr_id, date) %>%
    summarize(local_best_source = source[which.max(n_per_sourcedate)])


  multiples_single_source <- multiples %>%
    left_join(best_sources) %>%
    left_join(local_sources) %>%
    mutate(use_source = source %in% overall_best_source,
           use_source2 = source %in% local_best_source) %>%
    group_by(nhdhr_id, date, depth) %>%
    mutate(keep = ifelse(any(use_source), source[use_source],
                         ifelse(any(use_source2), source[use_source2], first(source)))) %>%
    ungroup() %>%
    filter(source == keep)


  # first, data we assume to be from multiple times at a single site
  # will have the same lake-source-site-date-depth
  # we will sort out these assumptions in more detail later on
  time_multiples <- multiples %>%
    group_by(nhdhr_id, source, source_site_id, date, depth) %>%
    mutate(n_fine = n()) %>%
    filter(n_fine > 1) %>%
    ungroup()

  # other multiples are resolved down to the source-site, but
  # have multiples across source-sites
  # this could be multiple sources with the same data, multiple sources
  # with different data, single source with multiple sites
  other_multiples <- multiples %>%
    group_by(nhdhr_id, source, source_site_id, date, depth) %>%
    mutate(n_fine = n()) %>%
    filter(n_fine == 1) %>%
    ungroup()



  best_sites <- multiples %>%
    group_by(nhdhr_id, source, source_site_id) %>%
    summarize(n_site_days = length(unique(date))) %>%
    group_by(nhdhr_id, source) %>%
    summarize(overall_best_site = source_site_id[which.max(n_site_days)]) %>%
    ungroup()


  #################################
  # sort out the "other" multiples

  # provide more info about where the multiplication is happening
  other_multiples <- group_by(other_multiples, nhdhr_id, date, depth) %>%
    mutate(n_sites = length(unique(source_site_id)),
           n_sources = length(unique(source))) %>%
    arrange(nhdhr_id, date, depth) %>%
    ungroup()

  # now reduce to single value for lake-date-depth

  # same source, multi sites -- # pick either site with most data
  # or first reported site, depending on which is available
  # this includes sites where you could have one declared and one undeclared
  # site ID (e.g., "site 1" and "NA")

  best_local_sites <- other_multiples %>%
    filter(n_sources == 1 & n_sites > 1) %>%
    group_by(nhdhr_id, source_site_id) %>%
    summarize(n_site_days = length(unique(date))) %>%
    group_by(nhdhr_id) %>%
    summarize(local_best_site = source_site_id[which.max(n_site_days)]) %>%
    ungroup()

  single_vals1 <- other_multiples %>%
    filter(n_sources == 1 & n_sites > 1) %>%
    arrange(nhdhr_id, date, depth) %>%
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

  # single lake-date-depth values, which have one site and one source
  # these have multiples when merged back with the "time_multiples" data
  single_vals2 <- other_multiples %>%
    filter(n_sources == 1 & n_sites == 1)

  # different sources, one to many sites
  # many of these are likely the same observations in two+ databases
  # so should first do a distinct()
  # picks the "best" source data (source with greatest ndays)
  # or the first source
  best_local_sites <- other_multiples %>%
    filter(n_sources > 1) %>%
    group_by(nhdhr_id, source, source_site_id) %>%
    summarize(n_site_days = length(unique(date))) %>%
    group_by(nhdhr_id) %>%
    summarize(local_best_site = source_site_id[which.max(n_site_days)]) %>%
    ungroup()

  best_local_sources <- other_multiples %>%
    filter(n_sources > 1) %>%
    group_by(nhdhr_id, source) %>%
    summarize(n_source_days = length(unique(date))) %>%
    group_by(nhdhr_id) %>%
    summarize(local_best_source = source[which.max(n_source_days)]) %>%
    ungroup()

  single_vals3 <- other_multiples %>%
    filter(n_sources > 1) %>%
    distinct(nhdhr_id, date, depth, temp, .keep_all = TRUE) %>%
    left_join(best_sources) %>%
    left_join(best_local_sources) %>%
    mutate(use_source = source %in% overall_best_source,
           use_source2 = source %in% local_best_source) %>%
    # first, pick the best source
    group_by(nhdhr_id, date, depth, source) %>%
    summarize(temp = ifelse(any(use_source), temp[use_source],
                            ifelse(any(use_source2), temp[use_source2], first(temp))),
              source_site_id = ifelse(any(use_source), source_site_id[use_source],
                                      ifelse(any(use_source2), source_site_id[use_source2], first(source_site_id))),
              source_id = first(source_id)) %>%
    # now if there are multiple sites in that source, pick best site
    ungroup() %>%
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


  resolved_other_multiples <- bind_rows(single_vals1, single_vals2, single_vals3)

  # insert message
  cat(nrow(other_multiples) - nrow(resolved_other_multiples), "temperature records were dropped due to
      multiple sources, multiple sites, multiple sources and sites, or duplicated records across sources
      per lake-date-depth. The 'best' sites and sources were kept, and was based on highest number of daily records.")


  ###################################
  # sort out "time" multiples
  # note that there may or may not be data in "times" column

  # handle data with no times reported
  # first, do we think these are multiple sites or multiple times or both?
  # first, handle data with no times + site information

  dat_notimes <- filter(time_multiples, is.na(time))
  dat_times <- filter(time_multiples, !is.na(time))

  ###### sort out "time" multiples with no time stamp

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
  # also, we deduce that if depths were measured a different amount of times
  # there is a better chance for those data to be from different sites (not times)
  dat_notimes_npersource <- filter(dat_notimes, is.na(source_site_id)) %>%
    filter(!grepl('manualentry', source)) %>%
    group_by(nhdhr_id, source, date, depth) %>%
    mutate(n_per_day = n()) %>%
    group_by(nhdhr_id, source, date) %>%
    mutate(same_depth = length(unique(as.numeric(summary(as.factor(depth))))) == 1) %>%
    arrange(nhdhr_id, date, depth)

  # what we assume to be different sites, take first record
  res3_dat_notimes <- filter(dat_notimes_npersource, !same_depth) %>%
    group_by(nhdhr_id, source, date, depth) %>%
    summarize(temp = first(temp),
              source_id = first(source_id))

  # what we assume to be different times, take middle record
  res4_dat_notimes <- filter(dat_notimes_npersource, same_depth & n_per_day >=12) %>%
    group_by(nhdhr_id, source, date, depth) %>%
    summarize(temp = temp[floor(mean(n_fine)/2) + 1],
              source_id = first(source_id))

  # the reasons for the rest of the repeated observations are unknown
  # take the value closest to the median of that lake-source-date-depth
  # if it's time, hopefully this captures mid day
  # if it's space, hopefully this captures the most representative site

  getmode <- function(v) {
    uniqv <- unique(v)
    uniqv[which.max(tabulate(match(v, uniqv)))]
  }

  res5_dat_notimes <- filter(dat_notimes_npersource, same_depth & n_per_day < 12) %>%
    group_by(nhdhr_id, source, date, depth) %>%
    mutate(closest_med = which.min(round(abs(temp - median(temp)), 2))) %>%
    group_by(nhdhr_id, source, date) %>%
    # make sure the same record is kept for each lake-source-date
    # in an attempt to keep the time or site the same across depths
    mutate(which_keep = getmode(closest_med)) %>%
    ungroup() %>%
    group_by(nhdhr_id, source, date, depth) %>%
    summarize(temp = temp[first(which_keep)],
              source_id = source_id[first(which_keep)])

  ############## sort out "time" data with time stamps

  # create a variable that represents difference from noon in hours
  # assume all UTC timezones are GMT-5

  # find local times closest to noon
  # adjust UTC to be central time (CDT) by subtracting 7 hours instead of 12
  # adjust GMT to be CDT by

  # FIX THIS
  # more time zones
  # need to account for pos or neg diff from noon
  dat_times <- dat_times %>%
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


