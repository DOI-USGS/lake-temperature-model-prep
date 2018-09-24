# munge wqp data
munge_temperature <- function(data.in){
  # from original lake temp repo: https://github.com/USGS-R/necsc-lake-modeling/blob/master/scripts/download_munge_wqp.R
  max.temp <- 40 # threshold!
  min.temp <- 0
  max.depth <- 260

  depth.unit.map <- data.frame(depth.units=c('meters','m','in','ft','feet','cm', 'mm', NA),
                               depth.convert = c(1,1,0.0254,0.3048,0.3048,0.01, 0.001, NA),
                               stringsAsFactors = FALSE)

  unit.map <- data.frame(units=c("deg C","deg F", NA),
                         convert = c(1, 1/1.8,NA),
                         offset = c(0,-32,NA),
                         stringsAsFactors = FALSE)

  activity.sites <- group_by(data.in, OrganizationIdentifier) %>%
    summarize(act.n = sum(!is.na(ActivityDepthHeightMeasure.MeasureValue)), res.n=sum(!is.na((ResultDepthHeightMeasure.MeasureValue)))) %>%
    mutate(use.depth.code = ifelse(act.n>res.n, 'act','res')) %>%
    select(OrganizationIdentifier, use.depth.code)

  left_join(data.in, activity.sites, by='OrganizationIdentifier') %>%
    mutate(raw.depth = as.numeric(ifelse(use.depth.code == 'act', ActivityDepthHeightMeasure.MeasureValue, ResultDepthHeightMeasure.MeasureValue)),
           depth.units = ifelse(use.depth.code == 'act', ActivityDepthHeightMeasure.MeasureUnitCode, ResultDepthHeightMeasure.MeasureUnitCode)) %>%
    rename(Date=ActivityStartDate,
           raw.value=ResultMeasureValue,
           units=ResultMeasure.MeasureUnitCode,
           wqx.id=MonitoringLocationIdentifier,
           timezone = ActivityStartTime.TimeZoneCode) %>%
    mutate(time = substr(ActivityStartTime.Time, 0, 5)) %>%
    select(Date, time, timezone, raw.value, units, raw.depth, depth.units, wqx.id) %>%
    left_join(unit.map, by='units') %>%
    left_join(depth.unit.map, by='depth.units') %>%
    mutate(wtemp=convert*(raw.value+offset), depth=raw.depth*depth.convert) %>%
    filter(!is.na(wtemp), !is.na(depth), wtemp <= max.temp, wtemp >= min.temp, depth <= max.depth) %>%
    select(Date, time, timezone, wqx.id, depth, wtemp)
}

munge_wqp_dat <- function(outind, wqp_ind) {

  outfile <- as_data_file(outind)

  wqp_in <- scipiper::sc_retrieve(wqp_ind)
  wqp_files <- feather::read_feather(wqp_in) %>%
    select(PullFile) %>%
    distinct() %>%
    pull()

  # loop through or apply a gd_get function to get all fipes in wqp_files
  # bind rows together
  wqp_dat <- data.frame()

  for (files in wqp_files) {
    temp_path <- file.path('6_temp_wqp_fetch', 'out', files)

    cat('Retrieving, munging, and binding WQP data from file', scipiper::as_data_file(temp_path), '\n')

    gd_get(temp_path)

    temp_dat <- feather::read_feather(as_data_file(temp_path))
    temp_dat_munged <- munge_temperature(temp_dat)

    perc_dropped <- round((nrow(temp_dat)-nrow(temp_dat_munged))/nrow(temp_dat)*100, 1)
    cat('Dropped ', nrow(temp_dat)-nrow(temp_dat_munged), " (", perc_dropped, "%) temperature observations during munge process.\n", sep = '')

    wqp_dat <- bind_rows(wqp_dat, temp_dat_munged)
  }

  wqp_dat <- distinct(wqp_dat)

  saveRDS(object = wqp_dat, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}

crosswalk_wqp_dat <- function(outind, wqp_munged, wqp_crosswalk) {

  outfile = as_data_file(outind)

  crossfile <- sc_retrieve(wqp_crosswalk, remake_file = '1_crosswalk_fetch.yml')
  wqp2nhd <- readRDS(crossfile)

  infile <- sc_retrieve(wqp_munged, remake_file = '7a_temp_wqp_munge.yml')
  wqp_dat <- readRDS(infile)

  wqp_linked <- left_join(wqp_dat, wqp2nhd, by = c('wqx.id' = 'MonitoringLocationIdentifier')) %>%
    select(-LatitudeMeasure, -LongitudeMeasure) %>%
    filter(!is.na(id))

  cat(nrow(wqp_dat) - nrow(wqp_linked), "temperature observations were dropped from WQP data for missing NHD lake identifiers.")

  feather::write_feather(wqp_linked, outfile)
  gd_put(outind, outfile)
}
