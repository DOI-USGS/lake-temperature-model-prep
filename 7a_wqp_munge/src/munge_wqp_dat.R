# munge wqp data
munge_wqp_temperature <- function(outind, wqp_ind){

  outfile <- as_data_file(outind)

  wqp_temp_data <- scipiper::sc_retrieve(wqp_ind) %>% readRDS()
  # from original lake temp repo: https://github.com/USGS-R/necsc-lake-modeling/blob/master/scripts/download_munge_wqp.R
  max_temp <- 40 # threshold!
  min_temp <- 0
  max_depth <- 260

  depth_unit_map <- data.frame(depth.units=c('meters','m','in','ft','feet','cm', 'mm', NA),
                               depth.convert = c(1,1,0.0254,0.3048,0.3048,0.01, 0.001, NA),
                               stringsAsFactors = FALSE)

  var_unit_map <- data.frame(units=c("deg C","deg F", NA),
                         convert = c(1, 1/1.8,NA),
                         offset = c(0,-32,NA),
                         stringsAsFactors = FALSE)

  activity.sites <- group_by(wqp_temp_data, OrganizationIdentifier) %>%
    summarize(act.n = sum(!is.na(`ActivityDepthHeightMeasure/MeasureValue`)), res.n=sum(!is.na((`ResultDepthHeightMeasure/MeasureValue`)))) %>%
    mutate(use.depth.code = ifelse(act.n>res.n, 'act','res')) %>%
    dplyr::select(OrganizationIdentifier, use.depth.code)

  d = left_join(wqp_temp_data, activity.sites, by='OrganizationIdentifier') %>%
    mutate(raw.depth = case_when(
        use.depth.code == 'act' ~ `ActivityDepthHeightMeasure/MeasureValue`,
        use.depth.code == 'res' ~ as.numeric(`ResultDepthHeightMeasure/MeasureValue`) #as of 10/25/2019, the chars that will fail conversion are things like "Haugen Lake Littoral", "Burns Lake Littoral", "Littoral Zone Sample"
      ),
      depth.units = case_when(
        use.depth.code == 'act' ~ `ActivityDepthHeightMeasure/MeasureUnitCode`,
        use.depth.code == 'res' ~ `ResultDepthHeightMeasure/MeasureUnitCode`
      )) %>%
    rename(Date = ActivityStartDate,
           raw_value = ResultMeasureValue,
           units = `ResultMeasure/MeasureUnitCode`,
           result_method = `ResultAnalyticalMethod/MethodIdentifier`,
           timezone = `ActivityStartTime/TimeZoneCode`) %>%
    mutate(time = substr(`ActivityStartTime/Time`, 0, 5)) %>%
    dplyr::select(Date, time, timezone, raw_value, units, raw.depth, depth.units, MonitoringLocationIdentifier, result_method) %>%
    left_join(var_unit_map, by='units') %>%
    left_join(depth_unit_map, by='depth.units') %>%
    mutate(wtemp = convert * (raw_value + offset), depth = raw.depth * depth.convert) %>%
    filter(!is.na(wtemp), !is.na(depth), wtemp <= max_temp, wtemp >= min_temp, depth <= max_depth, !result_method %in% c('LAB TEMP','LAB')) %>%
    dplyr::select(Date, time, timezone, MonitoringLocationIdentifier, depth, wtemp) %>%
    feather::write_feather(outfile)
  gd_put(outind, outfile)
}

munge_wqp_secchi <- function(outind, wqp_ind){

  outfile <- as_data_file(outind)

  wqp_data <- scipiper::sc_retrieve(wqp_ind) %>% readRDS()

  unit_map <- data.frame(units=c('m','in','ft','cm', NA),
                         convert = c(1,0.0254,0.3048,0.01, NA),
                         stringsAsFactors = FALSE)

  rename(wqp_data, Date=ActivityStartDate, value=ResultMeasureValue, units=`ResultMeasure/MeasureUnitCode`) %>%
    dplyr::select(Date, value, units, MonitoringLocationIdentifier) %>%
    left_join(unit_map, by='units') %>%
    mutate(secchi=value*convert) %>%
    filter(!is.na(secchi)) %>%
    dplyr::select(Date, MonitoringLocationIdentifier, secchi) %>%
    feather::write_feather(outfile)
  gd_put(outind, outfile)
}

crosswalk_wqp_dat <- function(outind, wqp_munged, wqp_crosswalk) {

  outfile = as_data_file(outind)

  crossfile <- sc_retrieve(wqp_crosswalk)
  wqp2nhd <- readRDS(crossfile) %>%
    distinct()

  infile <- sc_retrieve(wqp_munged)
  wqp_dat <- feather::read_feather(infile)

  wqp_linked <- left_join(wqp_dat, dplyr::select(wqp2nhd, site_id, MonitoringLocationIdentifier)) %>%
    rename(id = site_id) %>%
    filter(!is.na(id))

  cat(nrow(wqp_dat) - nrow(wqp_linked), "temperature observations were dropped from WQP data for missing NHD lake identifiers.")

  feather::write_feather(wqp_linked, outfile)
  gd_put(outind, outfile)
}
