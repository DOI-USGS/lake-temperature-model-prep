
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

subset_training_meteo <- function(filepath, filepath_in, start, stop){
  readr::read_csv(filepath_in) %>%
    filter(time <= as.Date(stop), time >= as.Date(start)) %>%
    feather::write_feather(path = filepath)
}

buoy_data <- function(){
  buoy_temp <- readr::read_csv("~/Downloads/ntl130_1_v5 (1).csv", skip = 1) %>%
    filter(is.na(flag_wtemp) | flag_wtemp == 'N',
           sampledate < as.Date('2009-07-03') | sampledate > as.Date('2009-07-06'),
           sampledate < as.Date('2014-06-17') | sampledate > as.Date('2014-07-10'),
           sampledate < as.Date('2008-11-11') | sampledate > as.Date('2008-12-12'),
           sampledate < as.Date('2014-07-06') | sampledate > as.Date('2014-10-20'),
           sampledate < as.Date('2011-08-07') | sampledate > as.Date('2011-08-19'),
           sampledate < as.Date('2012-08-31') | sampledate > as.Date('2012-09-04')) %>%
    select(DateTime = sampledate, Depth = depth, temp = wtemp)

  manual_temp <- readr::read_csv("~/Downloads/chemphys.csv", skip = 1) %>%
    filter(is.na(flagwtemp), !is.na(wtemp)) %>%
    group_by(sampledate, depth) %>% filter(row_number(wtemp) == 1) %>%
    select(DateTime = sampledate, Depth = depth, temp = wtemp)

  combined <- full_join(buoy_temp, manual_temp, by = c("DateTime", "Depth")) %>%
    mutate(temp = ifelse(is.na(temp.x), temp.y, temp.x)) %>% select(-temp.x, -temp.y) %>%
    arrange(DateTime, Depth)

  incomplete_profiles <- combined %>% group_by(DateTime) %>% tally() %>% filter(n < 3) %>% pull(DateTime)

  combined <- combined %>% filter(!DateTime %in% incomplete_profiles, DateTime > as.Date('2009-04-01'))
  readr::write_csv(buoy_temp, '../lake_modeling/data_imports/in/mendota_buoy.csv')
  readr::write_csv(combined, '../lake_modeling/data_imports/in/mendota_combined.csv')
  return(combined)
}

build_training_datasets <- function(n_experiments = 5, samples = c(800, 100, 50, 20, 10, 5, 2), test_range = as.Date(c('2011-12-01', '2016-04-01'))){

  training <- buoy_data() %>%
    filter(DateTime <= test_range[1] | DateTime >= test_range[2])

  un_dates <- training %>% pull(DateTime) %>% unique

  for (samp in samples){
    for (n in 1:n_experiments){
      n_exp <- stringr::str_pad(n, 2, pad = '0')
      feather_filepath <- sprintf('../lake_modeling/data_imports/training_data_mendota/Mendota_training_%sprofiles_experiment_%s.feather', samp, n_exp)
      figure_filepath <- sprintf('../lake_modeling/data_imports/figures/Mendota_training_%sprofiles_experiment_%s.png', samp, n_exp)
      set.seed(n)
      profile_dates <- sample(un_dates, samp, replace = FALSE)
      profiles <- training %>% filter(DateTime %in% profile_dates)
      subset_plots(figure_filepath, profiles)
      feather::write_feather(profiles, feather_filepath)
    }
  }
}

build_test_dataset <- function(test_range = as.Date(c('2011-12-01', '2016-04-01'))){
  feather_filepath <- '../lake_modeling/data_imports/test_data_mendota/Mendota_test.feather'
  figure_filepath <- '../lake_modeling/data_imports/figures/Mendota_test.png'
  test <- buoy_data() %>%
    filter(DateTime > test_range[1] & DateTime < test_range[2])

  subset_plots(figure_filepath, test, test_range)
  feather::write_feather(test, feather_filepath)

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


subset_plots <- function(filepath, profiles, xlim = as.Date(c('2009-04-01', '2017-12-17'))){

  temp_data <- profiles

  un_dates <- temp_data %>% pull(DateTime) %>% unique

  png(filename = filepath, width = 12, height = 3, units = 'in', res = 200)
  par(omi = c(0.4,0,0.05,0.05), mai = c(0,1,0,0))
  plot(xlim, c(NA,NA), xlim = xlim,
       ylim = c(24,0), xaxs = 'i', yaxs = 'i', ylab = 'Depth (m)', xlab = "")

  for (date in un_dates){
    data <- filter(temp_data, DateTime == date) %>% arrange()
    if (nrow(data) > 2){
      col = colorRampPalette(c("#00007F", "blue", "#007FFF", "cyan",
                               "#7FFF7F", "yellow", "#FF7F00", "red", "#7F0000"))(30)
      .filled.contour(x = c(date-0.5, date+0.5), y = data$Depth, z = rbind(data$temp,data$temp), levels = seq(0,to = 30), col = col)

    }

  }
  box()
  dev.off()
  #plot(buoy_temp$DateTime, buoy_temp$temp, xlim = as.Date(c('2009-04-10', '2013-11-01')), pch = 15, cex = 0.25)
}

train_test_overview_plots <- function(filepath, profiles, xlim = as.Date(c('2009-04-01', '2017-12-17'))){

  filepath <- '~/Downloads/test_train_mendota_option2.png'

  chunk_size <- 60
  temp_data <- profiles

  un_dates <- temp_data %>% pull(DateTime) %>% unique

  png(filename = filepath, width = 14, height = 4, units = 'in', res = 200)
  par(omi = c(0.25,0,0.05,0.05), mai = c(0.05,0.5,0,0), las = 1, mgp = c(2,.5,0))
  layout(matrix(1:5))

  plot(xlim, c(NA,NA), xlim = xlim,
       ylim = c(24,0), xaxs = 'i', yaxs = 'i', ylab = 'Depth (m)', xlab = "", axes = FALSE)

  axis(1, at = as.Date(c("2009-01-1","2010-01-01","2011-01-01","2012-01-01","2013-01-01","2014-01-01","2015-01-01","2016-01-01","2017-01-01")), labels = FALSE, tick = TRUE, tck = -0.02)
  axis(2, at = seq(0,25,10), tck = -0.02)

  un_dt_resample <- data.frame(date = un_dates, train = FALSE)
  set.seed(42)
  for (i in 1:10){
    good_sample <- FALSE
    while (!good_sample){
      start_i <- sample(1:nrow(un_dt_resample), 1)
      end_i <- start_i + chunk_size - 1
      if (end_i <= nrow(un_dt_resample) & !any(un_dt_resample$train[start_i:end_i])){
        good_sample = TRUE
        un_dt_resample$train[start_i:end_i] <- TRUE
        rect(xleft = un_dt_resample$date[start_i], ybottom = 25, xright = un_dt_resample$date[end_i], ytop = 0, col = '#0c0c0c47', border = NA)
      } else {
        message('moving on')
      }
    }
  }

  for (date in un_dates){
    data <- filter(temp_data, DateTime == date) %>% arrange()
    if (nrow(data) > 2){
      col = colorRampPalette(c("#00007F", "blue", "#007FFF", "cyan",
                               "#7FFF7F", "yellow", "#FF7F00", "red", "#7F0000"))(30)
      .filled.contour(x = c(date-0.5, date+0.5), y = data$Depth, z = rbind(data$temp,data$temp), levels = seq(0,to = 30), col = col)

    }

  }
  text(as.Date("2009-04-10"), y = 4, "entire dataset", pos = 4)
  box()

  build_subset <- function(n){
    plot(xlim, c(NA,NA), xlim = xlim,
         ylim = c(24,0), xaxs = 'i', yaxs = 'i', ylab = 'Depth (m)', xlab = "", axes = FALSE)
    axis(1, at = as.Date(c("2009-01-1","2010-01-01","2011-01-01","2012-01-01","2013-01-01","2014-01-01","2015-01-01","2016-01-01","2017-01-01")), labels = FALSE, tick = TRUE, tck = -0.02)
    axis(2, at = seq(0,25,10), tck = -0.02)

    profile_dates <- sample(un_dt_resample$date[!un_dt_resample$train], n, replace = FALSE)
    for (date in profile_dates){
      data <- filter(temp_data, DateTime == date) %>% arrange()
      if (nrow(data) > 2){
        col = colorRampPalette(c("#00007F", "blue", "#007FFF", "cyan",
                                 "#7FFF7F", "yellow", "#FF7F00", "red", "#7F0000"))(30)
        .filled.contour(x = c(date-0.5, date+0.5), y = data$Depth, z = rbind(data$temp,data$temp), levels = seq(0,to = 30), col = col)

      }

    }
    text(as.Date("2009-04-10"), y = 4, sprintf("training n=%s", n), pos = 4)
    box()
  }

  build_subset(920)
  build_subset(100)
  build_subset(50)
  build_subset(10)

  axis(1, at = as.Date(c("2009-01-1","2010-01-01","2011-01-01","2012-01-01","2013-01-01","2014-01-01","2015-01-01","2016-01-01","2017-01-01")), labels = 2009:2017, tick = TRUE, tck = -0.02)

  dev.off()

}


