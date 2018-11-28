
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
