

read_aoss_solar <- function(url){

  # get the year from the URL...
  warning('not completely implemented yet')
  col_names <- get_aoss_cols(year = 2009)
  readr::read_table(url, skip = 2, col_names = col_names, col_types = readr::cols()) %>%
    group_by(hour) %>% summarize(
      date_string = sprintf('%s-%s-%s %s:00', unique(year), unique(month), unique(day), unique(hour)),
      datetime = as.POSIXct(date_string, tz = 'UTC'),
      ShortWave = mean(dw_psp),
      LongWave = mean(dpir),
      sw_qc = sum(qc_dwpsp),
      lw_qc = sum(qc_dpir)) %>% # fail on < 2009...
    select(datetime, ShortWave, LongWave, sw_qc, lw_qc)


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



is.leapyear=function(year){
  #http://en.wikipedia.org/wiki/Leap_year
  return(((year %% 4 == 0) & (year %% 100 != 0)) | (year %% 400 == 0))
}

ready_files <- c()

missing_files <- c('msn10162.feather','msn17174.feather')

for (year in 2009:2018){
  last_doy <- ifelse(is.leapyear(year), 366, 365)
  for (doy in 1:last_doy){
    doy_padded <- stringr::str_pad(doy, width = 3, pad = "0", side = "left")
    yy <- as.character(year) %>% substr(3, 4) # only works for > 2000
    url <- sprintf('ftp://aftp.cmdl.noaa.gov/data/radiation/solrad/msn/%s/msn%s%s.dat', year, yy, doy_padded)
    filename <- sprintf('msn%s%s.feather', yy, doy_padded)
    filepath <- file.path('6_drivers/out/aoss_dat/', filename)
    if (!file.exists(filepath)){
      if (filename %in% missing_files){
        message('skipping known missing file')
      } else {
        data_out <- read_aoss_solar(url)
        feather::write_feather(data_out, path = filepath)
        ready_files <- c(ready_files, filepath)
        message('written ', sprintf('msn%s%s.feather', yy, doy_padded))
      }

    } else {
      ready_files <- c(ready_files, filepath)
      message('**skipping ', sprintf('msn%s%s.feather **', yy, doy_padded))
    }
  }
}

drivers_in <- data.frame(stringsAsFactors = FALSE)
for (feather_filepath in ready_files){
  data <- feather::read_feather(feather_filepath)
  drivers_in <- rbind(drivers_in, data)
}

# simple QAQC:

aoss_qaqc <- filter(drivers_in, lw_qc == 0, sw_qc == 0)

nldas_sw_filepath <- '6_drivers/out/feather/NLDAS_time[0.346848]_x[284]_y[144]_var[dswrfsfc].feather'
nldas_lw_filepath <- '6_drivers/out/feather/NLDAS_time[0.346848]_x[284]_y[144]_var[dlwrfsfc].feather'
nldas_u10_filepath <- '6_drivers/out/feather/NLDAS_time[0.346848]_x[284]_y[144]_var[ugrd10m].feather'
nldas_v10_filepath <- '6_drivers/out/feather/NLDAS_time[0.346848]_x[284]_y[144]_var[vgrd10m].feather'
nldas_air_filepath <- '6_drivers/out/feather/NLDAS_time[0.346848]_x[284]_y[144]_var[tmp2m].feather'


NLDAS_start <- as.POSIXct('1979-01-01 13:00', tz = "UTC")
all_time_vec <- seq(NLDAS_start, by = 'hour', to = Sys.time())
drivers_nldas <- data.frame(time = all_time_vec[1:346849], stringsAsFactors = FALSE)

# now get Mendota feather for these variables:
drivers_nldas$me_sw <- feather::read_feather(nldas_sw_filepath) %>% pull(dswrfsfc)
drivers_nldas$me_lw <- feather::read_feather(nldas_lw_filepath) %>% pull(dlwrfsfc)
drivers_nldas$me_u10 <- feather::read_feather(nldas_u10_filepath) %>% pull(ugrd10m)
drivers_nldas$me_v10 <- feather::read_feather(nldas_v10_filepath) %>% pull(vgrd10m)
drivers_nldas$me_air <- feather::read_feather(nldas_air_filepath) %>% pull(tmp2m)
hourly_nldas <- mutate(drivers_nldas, date = lubridate::as_date(time)) %>%
  mutate(wnd = sqrt(me_u10^2 + me_v10^2), air = me_air - 273.15)

daily_drivers <- mutate(aoss_qaqc, date = lubridate::as_date(datetime)) %>%
  group_by(date) %>%
  summarize(LW = mean(LongWave), SW = mean(ShortWave), AIR = mean(me_air))


daily_nldas <- mutate(drivers_nldas, date = lubridate::as_date(time)) %>%
  mutate(wnd = sqrt(me_u10^2 + me_v10^2)) %>% group_by(date) %>%
  summarize(LW_nldas = mean(me_lw), SW_nldas = mean(me_sw), WND_nldas = mean(wnd^3)^(1/3))

combined <- inner_join(hourly_nldas, daily_drivers, by = 'date')

plot(combined$date, combined$LW, ylim = c(100,500), xlim = as.Date(c('2016-06-01','2018-08-01')), ylab = 'radiation (W/m2)', xlab = "")
points(combined$date, combined$LW_nldas, pch = 20, col = 'red')

plot(combined$LW, combined$LW_nldas, xlim = c(100, 500), ylim = c(100,500), asp = 1, ylab = 'NLDAS LW radiation (W/m2)', xlab = 'AOSS LW radiation (W/m2)')
abline(0,1)

plot(combined$SW, combined$SW_nldas, xlim = c(0, 400), ylim = c(0,400), asp = 1, ylab = 'NLDAS SW radiation (W/m2)', xlab = 'AOSS SW radiation (W/m2)')
abline(0,1)

mendota_buoy <- readr::read_csv("~/Downloads/ntl129_2_v5.csv") %>% filter(!is.na(hour)) %>%
  mutate(time = as.POSIXct(sprintf("%s %s:00", sampledate, hour/100)))

plot(hourly_nldas$time, hourly_nldas$wnd, xlim = as.POSIXct(c('2017-05-01','2017-08-01')))
points(mendota_buoy$time, mendota_buoy$avg_wind_speed, col = 'red')


plot(hourly_nldas$time, hourly_nldas$me_air, xlim = as.POSIXct(c('2017-05-01','2017-08-01')))
points(mendota_buoy$time, mendota_buoy$avg_air_temp, col = 'red')


combined_buoy <- inner_join(hourly_nldas, mendota_buoy, by = 'time') %>% mutate(date = lubridate::as_date(time)) %>%
  group_by(date) %>%
  summarize(wnd_nldas = mean(wnd^3)^(1/3), wnd_buoy = mean(avg_wind_speed^3)^(1/3), air_nldas = mean(air), air_buoy = mean(avg_air_temp))

plot(combined_buoy$wnd_buoy, combined_buoy$wnd_nldas, xlim = c(0, 12), ylim = c(0,12), asp = 1, ylab = 'NLDAS windspeed (m/s)', xlab = 'buoy windspeed (m/s)');abline(0,1)

plot(combined_buoy$air_buoy, combined_buoy$air_nldas, xlim = c(-20, 30), ylim = c(-20,30), asp = 1, ylab = 'NLDAS air temp (°C)', xlab = 'buoy air temp (°C)');abline(0,1)
