
fetch_aoss_rig <- function(filepath, url){
  nc_path <- file.path(tempdir(), 'rig.nc')
  download.file(url, nc_path, quiet = TRUE)
  nc <- ncdf4::nc_open(nc_path)
  time_start <- ncdf4::ncvar_get(nc, "base_time") %>% as.POSIXct(origin = '1970-01-01', tz = 'UTC')
  time_vec <- ncdf4::ncvar_get(nc, "time_offset")
  data_out <- data.frame(time = time_start + time_vec, stringsAsFactors = FALSE) %>%
    mutate(RH = ncdf4::ncvar_get(nc, 'rh'),
           RH_qc = ncdf4::ncvar_get(nc, 'qc_rh'),
           airtemp = ncdf4::ncvar_get(nc, 'air_temp'),
           airtemp_qc = ncdf4::ncvar_get(nc, 'qc_air_temp'),
           windspeed = ncdf4::ncvar_get(nc, 'wind_speed'),
           windspeed_qc = ncdf4::ncvar_get(nc, 'qc_wind_speed'),
           pressure = ncdf4::ncvar_get(nc, 'pressure'),
           pressure_qc = ncdf4::ncvar_get(nc, 'qc_pressure')) %>%
    mutate(hour = format(time, '%H')) %>% group_by(hour) %>%
    summarize(datetime = time[1L],
              RH = mean(RH), RH_qc = sum(RH_qc),
              airtemp = mean(airtemp), airtemp_qc = sum(airtemp_qc),
              windspeed = mean(windspeed), windspeed_qc = sum(windspeed_qc),
              pressure = mean(pressure), pressure_qc = sum(pressure_qc)) %>%
    select(-hour)

  ncdf4::nc_close(nc)
  unlink(nc_path)
  feather::write_feather(data_out, path = filepath)
}


fetch_aoss_solar <- function(filepath, url){

  col_names <- get_aoss_cols(year = 2009)
  data_out <- readr::read_table(url, skip = 2, col_names = col_names, col_types = readr::cols()) %>%
    group_by(hour) %>% summarize(
      date_string = sprintf('%s-%s-%s %s:00', unique(year), unique(month), unique(day), unique(hour)),
      datetime = as.POSIXct(date_string, tz = 'UTC'),
      ShortWave = mean(dw_psp),
      LongWave = mean(dpir),
      sw_qc = sum(qc_dwpsp),
      lw_qc = sum(qc_dpir)) %>% # fail on < 2009...
    select(datetime, ShortWave, LongWave, sw_qc, lw_qc)

  feather::write_feather(data_out, path = filepath)
}

get_aoss_cols <- function(year){
  if (year < 2009){
    c('year','jday','month','day','hour','min','dt', 'zen', 'dw_psp', 'qc_dwpsp', 'direct', 'qc_direct',
      'diffuse', 'qc_diffuse', 'uvb', 'qc_uvb', 'uvb_temp', 'qc_uvb_temp', 'std_dw_psp', 'std_direct', 'std_diffuse', 'std_uvb')
  } else {
    c('year','jday','month','day','hour','min','dt', 'zen', 'dw_psp', 'qc_dwpsp', 'direct', 'qc_direct',
      'diffuse', 'qc_diffuse', 'uvb', 'qc_uvb', 'uvb_temp', 'qc_uvb_temp', 'dpir', 'qc_dpir', 'dpirc', 'qc_dpirc', 'dpird',
      'qc_dpird', 'std_dw_psp', 'std_direct', 'std_diffuse', 'std_uvb', 'std_dpir', 'std_dpirc', 'std_dpird')
  }

}


