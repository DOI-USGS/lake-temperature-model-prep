merge_temp_data <- function(outind, wqp_ind, coop_ind) {

  outfile <- as_data_file(outind)

  wqp_in <- sc_retrieve(wqp_ind)
  wqp_dat <- feather::read_feather(wqp_in)

  coop_in <- sc_retrieve(coop_ind)
  coop_dat <- feather::read_feather(coop_in)

  total_obs <- nrow(wqp_dat) + nrow(coop_dat)

  # merge and remove duplicate observations
  all_dat <- select(wqp_dat, date = Date, time, timezone, depth, temp = wtemp, nhd_id = id, source_id = wqx.id) %>%
    mutate(source = 'wqp') %>%
    bind_rows(select(coop_dat, date = DateTime, time, timezone, depth, temp, nhd_id = site_id, source_id = state_id, source)) %>%
    mutate(depth = round(depth, 2),
           depth1 = round(depth, 1),
           temp = round(temp, 2),
           temp1 = round(temp, 1)) %>%
    distinct()

  all_dat_distinct <- all_dat %>%
    mutate(timezone = ifelse(is.na(time), NA, timezone)) %>%
    distinct(nhd_id, date, time, depth1, temp1, .keep_all = TRUE) %>%
    select(-depth1, -temp1)

  # note - could still have lake/date/depth duplicates here
  # the risk of removing time from the above distinct() is that if you have
  # multiple observations per day, you could inadverdently lose obs that are distinct
  # in time but happen to have the same values (e.g., noon and 1pm surface samples that measure
  # the same temperature). The risk of not removing them is that you have multiple data sources
  # repeating the same data - but may not have a time stamp, for example. Will keep for now, sort out
  # in next step where we resolve time.

  cat(total_obs - nrow(all_dat_distinct), 'temperature observations were duplicated across WQP and cooperator data files and removed.')

  feather::write_feather(all_dat_distinct, outfile)
  gd_put(outind, outfile)

}

reduce_temp_data <- function() {

  # for lake/station/date/depth that have multiple observations per day,
  # filter to time closest to noon
  outfile <- as_data_file(outind)

  all_in <- sc_retrieve(inind)
  all_dat <- feather::read_feather(all_in)

  # handle data with no times reported - check assumption that this subset does not
  # have repeated values

  dat_notimes <- filter(all_dat, is.na(time)) %>%
    distinct(nhd_id, date, depth, temp, .keep_all = TRUE)

  # for lakes from same source that have multiple site IDs
  test_notimes <- group_by(dat_notimes, nhd_id, source_id, date, depth) %>%
    mutate(n = n()) %>%
    filter(n >1) %>%
    arrange(nhd_id, source_id, date, depth)

  dat_no
  # get data with time
  # create a variable that represents difference from noon in hours
  # assume all UTC timezones are GMT-5




  # find local times closest to noon
  # adjust UTC to be central time by subtracting 7 hours instead of 12
  dat_times <- filter(all_dat, !is.na(time)) %>%
    mutate(time_hm = lubridate::hm(time)) %>%
    mutate(hours_diff_noon = case_when(timezone %in% 'UTC' ~ abs(time_hm$hour - 7) + (time_hm$minute/60),
                                     TRUE ~ abs(time_hm$hour - 12) - (time_hm$minute/60)))

  # group by lake, siteid, date, depth and take the time closest to noon
  dat_singletimes <- dat_times %>%
    group_by(nhd_id, source, source_id,  date, depth) %>%
    summarize(temp = temp[which.min(hours_diff_noon)],
              time = time[which.min(hours_diff_noon)],
              ntimeobs = n()) %>%
    distinct(nhd_id, date, depth, temp, .keep_all = TRUE) %>%
    ungroup() %>%
    arrange(nhd_id, date, depth)


  all_dat_singletimes <- bind_rows(select(dat_notimes, -time, -timezone),
                                   select(dat_singletimes, -time, -ntimeobs)) %>%
    distinct(nhd_id, date, depth, temp, .keep_all = TRUE)

  # average across sites
  # still have multiple times/day because we grouped by source (no way to know if same site)
  # but should not have more than 1 obs/time/data source
  # now average across "sites" since data are distinct.
  # if this assumption is wrong, the very worst we're doing is average across small rounding errors

  all_dat_singletimes_singlesites <- group_by(all_dat_singletimes, nhd_id, date, depth) %>%
    summarize(temp = mean(temp),
              n = n())

  # how many observations had to be averaged?
  nrow(filter(all_dat_singletimes))

  # lake that has 24 observations on day
  View(filter(all_dat, nhd_id == 'nhd_2360642' & date == as.Date('1992-10-30')))

  feather::write_feather(all_dat_singletimes, outfile)
  gd_put(outind, outfile)

}


check_temp_data <- function() {
  # this function should apply some rules to the merged temp data
  # data between 0-40 degrees
  # no duplicate observations, see David's message about near-duplicates and rounding
  # taking the average if we have same day measurements from a lake/depth combo that are clearly different?
  # summer/winter checks? flag as unlikely data?
  # push data to gd as (single?) file
}
