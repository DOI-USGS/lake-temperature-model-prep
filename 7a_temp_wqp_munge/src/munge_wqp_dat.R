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
    dplyr::select(OrganizationIdentifier, use.depth.code)

  left_join(data.in, activity.sites, by='OrganizationIdentifier') %>%
    mutate(raw.depth = as.numeric(ifelse(use.depth.code == 'act', ActivityDepthHeightMeasure.MeasureValue, ResultDepthHeightMeasure.MeasureValue)),
           depth.units = ifelse(use.depth.code == 'act', ActivityDepthHeightMeasure.MeasureUnitCode, ResultDepthHeightMeasure.MeasureUnitCode)) %>%
    rename(Date=ActivityStartDate,
           raw.value=ResultMeasureValue,
           units=ResultMeasure.MeasureUnitCode,
           wqx.id=MonitoringLocationIdentifier,
           timezone = ActivityStartTime.TimeZoneCode) %>%
    mutate(time = substr(ActivityStartTime.Time, 0, 5)) %>%
    dplyr::select(Date, time, timezone, raw.value, units, raw.depth, depth.units, wqx.id) %>%
    left_join(unit.map, by='units') %>%
    left_join(depth.unit.map, by='depth.units') %>%
    mutate(wtemp=convert*(raw.value+offset), depth=raw.depth*depth.convert) %>%
    filter(!is.na(wtemp), !is.na(depth), wtemp <= max.temp, wtemp >= min.temp, depth <= max.depth) %>%
    dplyr::select(Date, time, timezone, wqx.id, depth, wtemp)
}

munge_wqp_dat <- function(outind, wqp_ind) {

  outfile <- as_data_file(outind)

  wqp_in <- scipiper::sc_retrieve(wqp_ind)
  wqp_files <- feather::read_feather(wqp_in) %>%
    dplyr::select(PullFile) %>%
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

crosswalk_wqp_dat <- function(outind, wqp_munged, wqp_crosswalk, wqp_latlong_ind) {

  outfile = as_data_file(outind)

  crossfile <- sc_retrieve(wqp_crosswalk)
  wqp2nhd <- readRDS(crossfile) %>%
    distinct()

  wqp_latlong <- readRDS(sc_retrieve(wqp_latlong_ind))

  latlong <- as.data.frame(st_coordinates(wqp_latlong)) %>%
    mutate(MonitoringLocationIdentifier = wqp_latlong$MonitoringLocationIdentifier) %>%
    rename(LongitudeMeasure = X, LatitudeMeasure = Y) %>%
    distinct()

  wqp_nhdLookup <- left_join(wqp2nhd, latlong)

  infile <- sc_retrieve(wqp_munged)
  wqp_dat <- readRDS(infile)

  wqp_linked <- left_join(wqp_dat, wqp_nhdLookup, by = c('wqx.id' = 'MonitoringLocationIdentifier')) %>%
    dplyr::select(-LatitudeMeasure, -LongitudeMeasure) %>%
    rename(id = site_id) %>%
    filter(!is.na(id))

  cat(nrow(wqp_dat) - nrow(wqp_linked), "temperature observations were dropped from WQP data for missing NHD lake identifiers.")

  feather::write_feather(wqp_linked, outfile)
  gd_put(outind, outfile)
}
