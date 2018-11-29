
create_aoss_meteo_tasks <- function(aoss_dates, skip_solar, skip_rig){

  date_range <- as.Date(aoss_dates)

  tasks <- seq(date_range[1], date_range[2], by = 'days')


  solar_ascii <- scipiper::create_task_step(
    step_name = 'solar_ascii',
    target_name = function(task_name, step_name, ...){

      sprintf("6_drivers/out/aoss_dat/solar_%s.feather", task_name)
    },
    command = function(task_name, ...){
      task_name <- as.Date(task_name)
      year <- format(task_name, '%Y')
      doy <- as.numeric(task_name) - as.numeric(as.Date(sprintf('%s-01-01', year))) +1
      doy_padded <- stringr::str_pad(doy, width = 3, pad = "0", side = "left")
      yy <- substr(year, 3, 4) # only works for > 2000
      url <- sprintf('ftp://aftp.cmdl.noaa.gov/data/radiation/solrad/msn/%s/msn%s%s.dat', year, yy, doy_padded)

      sprintf("fetch_aoss_solar(target_name, I('%s'))", url)
    }
  )

  rig_netcdf <- scipiper::create_task_step(
    step_name = 'rig_netcdf',
    target_name = function(task_name, step_name, ...){

      sprintf("6_drivers/out/aoss_nc/rig_%s.feather", task_name)
    },
    command = function(task_name, ...){

      task_name <- as.Date(task_name)
      year <- format(task_name, '%Y')
      month <- format(task_name, '%m') %>% stringr::str_pad(width = 2, pad = "0", side = "left")
      day <- format(task_name, '%d') %>% stringr::str_pad(width = 2, pad = "0", side = "left")
      url <- sprintf('http://metobs.ssec.wisc.edu/pub/cache/aoss/tower/level_b1/version_00/%s/%s/%s/aoss_tower.%s.nc', year, month, day, task_name)

      sprintf("fetch_aoss_rig(target_name, I('%s'))", url)
    }
  )

  step_list <- list(solar_ascii, rig_netcdf)
  gif_task_plan <- scipiper::create_task_plan(
    task_names=format(tasks, '%Y-%m-%d'),
    task_steps=step_list,
    add_complete=FALSE,
    final_steps='rig_netcdf',
    ind_dir="6_drivers/log")
}

create_aoss_meteo_makefile <- function(makefile, task_plan, remake_file) {

  scipiper::create_task_makefile(
    makefile=makefile, task_plan=task_plan,
    include=remake_file,
    packages=c('dplyr', 'scipiper', 'readr', 'ncdf4'),
    sources=c(
      '6_drivers/src/aoss_solar_utils.R'),
    file_extensions=c('feather','ind'),
    ind_complete=TRUE)
}

combine_solrad_files <- function(filepath){
  files <- dir('6_drivers/out/aoss_dat/')
  data_out <- data.frame()
  for (file in files){
    filename <- file.path('6_drivers/out/aoss_dat/', file)
    data_in <- feather::read_feather(filename)
    data_out <- rbind(data_out, data_in)
  }
  solrad_meteo <- data_out %>%
    mutate(time = lubridate::as_date(datetime, tz = "Etc/GMT+6")) %>%  # to local time for day averaging
    group_by(time) %>%
    summarize(ShortWave = mean(ShortWave), ShortWave_qc = sum(sw_qc) > 0,
              LongWave = mean(LongWave), LongWave_qc = sum(lw_qc) > 0 | LongWave < 0)

  feather::write_feather(solrad_meteo, filepath)
}


combine_rig_files <- function(filepath){
  files <- dir('6_drivers/out/aoss_nc/')
  data_out <- data.frame()
  for (file in files){
    filename <- file.path('6_drivers/out/aoss_nc/', file)
    data_in <- feather::read_feather(filename)
    data_out <- rbind(data_out, data_in)
  }
  rig_meteo <- data_out %>%
    mutate(windspeed = windspeed*0.92 - 0.03233,
           RH = RH*0.7535 + 19.539,
           airtemp = airtemp*0.954 + 1.252,
           time = lubridate::as_date(datetime, tz = "Etc/GMT+6")) %>%  # to local time for day averaging
  group_by(time) %>%
    summarize(WindSpeed = mean(windspeed^3)^(1/3), WindSpeed_qc = sum(windspeed_qc) > 0,
              AirTemp = mean(airtemp), AirTemp_qc = sum(airtemp_qc) > 0,
              RelHum = mean(RH), RelHum_qc = sum(RH_qc) > 0)

  feather::write_feather(rig_meteo, filepath)
}

combine_nldas_files <- function(filepath, ...){

  NLDAS_start <- as.POSIXct('1979-01-01 13:00', tz = "UTC")
  all_time_vec <- seq(NLDAS_start, by = 'hour', to = Sys.time())
  file_times <- c(1, 346849)
  drivers_in <- data.frame(time = all_time_vec[file_times[1]:file_times[2]], stringsAsFactors = FALSE)

  feather_filepaths <- c(...)
  for (feather_filepath in feather_filepaths){
    data <- read_feather(feather_filepath)
    drivers_in <- cbind(drivers_in, data)
  }

  # https://github.com/USGS-R/mda.lakes/blob/8e4925e63f1403e0802162437373fe62b243d024/R/get_driver_nhd.R
  # turn into actual driver vars:
  drivers_out <- drivers_in %>% rename(ShortWave = dswrfsfc, LongWave = dlwrfsfc) %>%
    mutate(WindSpeed = sqrt(ugrd10m^2 + vgrd10m^2), AirTemp = tmp2m - 273.15,
           RelHum = 100*spfh2m/qsat(AirTemp, pressfc*0.01),
           Rain = apcpsfc*24/1000) %>%
    mutate(Snow = ifelse(AirTemp < 0, Rain*10, 0), Rain = ifelse(AirTemp < 0, 0, Rain)) %>% #convert to m/day rate)
    select(time, ShortWave, LongWave, AirTemp, RelHum, WindSpeed, Rain, Snow) %>% # now downsample?
    mutate(date = lubridate::as_date(time, tz = "Etc/GMT+6")) %>% group_by(date) %>%
    summarize(ShortWave = mean(ShortWave), LongWave = mean(LongWave),
              AirTemp = mean(AirTemp), RelHum = mean(RelHum),
              WindSpeed = mean(WindSpeed^3)^(1/3), Rain = mean(Rain), Snow = mean(Snow), n = length(time)) %>%
    filter(n == 24) %>% rename(time = date) %>% select(-n)

  stopifnot(length(unique(diff(drivers_out$time))) == 1)

  feather::write_feather(drivers_out, filepath)
}

merge_aoss_nldas_meteo <- function(filepath, nldas_filepath, radiation_filepath, weather_filepath){

  radiation_data <- read_feather(radiation_filepath)
  weather_data <- read_feather(weather_filepath)

  meteo_data <- feather::read_feather(nldas_filepath) %>%
    full_join(radiation_data, by = 'time') %>%
    full_join(weather_data, by = 'time') %>%
    mutate(ShortWave = ifelse(!is.na(ShortWave.y) & !ShortWave_qc, ShortWave.y, ShortWave.x),
           LongWave = ifelse(!is.na(LongWave.y) & !LongWave_qc, LongWave.y, LongWave.x),
           AirTemp = ifelse(!is.na(AirTemp.y) & !AirTemp_qc, AirTemp.y, AirTemp.x),
           RelHum = ifelse(!is.na(RelHum.y) & !RelHum_qc, RelHum.y, RelHum.x),
           WindSpeed = ifelse(!is.na(WindSpeed.y) & !WindSpeed_qc, WindSpeed.y, WindSpeed.x)) %>%
    select(time,ShortWave,LongWave,AirTemp,RelHum,WindSpeed,Rain,Snow) %>%
    arrange(time) %>%
    filter(time < as.Date("2018-07-27"), time >= as.Date("2009-01-01")) # HARDCODED - this is the end of our NLDAS pull and beginning of station data

  readr::write_csv(x = meteo_data, path = filepath)
}


buoy_data <- function(){
  buoy_temp <- readr::read_csv("~/Downloads/ntl130_1_v5 (1).csv", skip = 1) %>%
    filter(is.na(flag_wtemp) | flag_wtemp == 'N',
           sampledate < as.Date('2009-07-03') | sampledate > as.Date('2009-07-06'),
           sampledate < as.Date('2014-06-17') | sampledate > as.Date('2014-07-10'),
           sampledate < as.Date('2008-11-11') | sampledate > as.Date('2008-12-12'),
           sampledate < as.Date('2012-08-31') | sampledate > as.Date('2012-09-04')) %>%
    select(DateTime = sampledate, Depth = depth, temp = wtemp)

  readr::write_csv(buoy_temp, '../lake_modeling/data_imports/in/mendota_buoy.csv')
}

diagnostics <- function(){
  mendota_buoy <- readr::read_csv("~/Downloads/ntl129_2_v5.csv") %>% filter(!is.na(hour)) %>%
         mutate(datetime = as.POSIXct(sprintf("%s %s:00", sampledate, hour/100)))

  # coefficients solved via lm after removing qc-flagged data:
  combined_buoy <- inner_join(data_out, mendota_buoy, by = 'datetime') %>% mutate(date = lubridate::as_date(datetime)) %>%
    mutate(windspeed = windspeed*0.92 - 0.03233,
           RH = RH*0.7535 + 19.539,
           airtemp = airtemp*0.954 + 1.252) %>%
    group_by(date) %>%
    summarize(wnd_rig = mean(windspeed^3)^(1/3), wnd_buoy = mean(avg_wind_speed^3)^(1/3), wnd_qc = sum(windspeed_qc) > 0,
              air_rig = mean(airtemp), air_buoy = mean(avg_air_temp), air_qc = sum(airtemp_qc) > 0,
              rh_rig = mean(RH), rh_buoy = mean(avg_rel_hum), rh_qc = sum(RH_qc) > 0)
  plot(combined_buoy$wnd_buoy[!combined_buoy$wnd_qc], combined_buoy$wnd_rig[!combined_buoy$wnd_qc], asp = 1, ylab = 'AOSS windspeed (m/s)', xlab = 'Buoy windspeed (m/s)'); abline(0,1)
  plot(combined_buoy$rh_buoy[!combined_buoy$rh_qc], combined_buoy$rh_rig[!combined_buoy$rh_qc], asp = 1, ylab = 'AOSS RH (%)', xlab = 'Buoy RH (%)'); abline(0,1)
  plot(combined_buoy$air_buoy[!combined_buoy$air_qc], combined_buoy$air_rig[!combined_buoy$air_qc], asp = 1, ylab = 'AOSS air temperature (°C)', xlab = 'Buoy air temperature (°C)'); abline(0,1)
}
