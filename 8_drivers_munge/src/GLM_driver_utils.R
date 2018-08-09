calc_feather_ind_files <- function(grid_cells, time_range, ind_dir){
  cell_list_to_df(grid_cells) %>%
    mutate(t0 = time_range[1], t1 = time_range[2]) %>%
    mutate(filepath = paste0(create_feather_filename(t0, t1, x = x, y = y, variable = variable, dirname = ind_dir), '.ind')) %>%
    pull(filepath)
}


calc_driver_files <- function(feather_ind_files, dirname){
  driver_files <- rep(NA_character_, length(feather_ind_files))
  for (i in seq_len(length(feather_ind_files))){
    ind_file <- feather_ind_files[i]
    times <- parse_feather_filename(ind_file, out = 'time')
    x <- parse_feather_filename(ind_file, out = 'x')
    y <- parse_feather_filename(ind_file, out = 'y')
    driver_files[i] <- create_meteo_filepath(times[1], times[2], x, y, dirname = dirname)
  }
  return(unique(driver_files))
}


create_meteo_filepath <- function(t0, t1, x, y, prefix = 'NLDAS', dirname){
  file.path(dirname, sprintf("%s_time[%1.0f.%1.0f]_x[%1.0f]_y[%1.0f].csv", prefix, t0, t1, x, y))
}

parse_meteo_filepath <- function(filepath, out = c('y','x','time')){

  word_indx <- switch(out,
                      y = 3,
                      x = 2,
                      time = 1)
  char <- gsub("\\[|\\]", "", regmatches(filepath, gregexpr("\\[.*?\\]", filepath))[[1]][word_indx])
  return(as.numeric(strsplit(char, '[.]')[[1]]))
}


create_driver_task_plan <- function(driver_files, feather_ind_files, data_dir, ind_dir){

  driver_task_step <- create_task_step(
    step_name = 'munge_drivers',
    target = function(task_name, step_name, ...) {
      file.path(out_dir, task_name)
    },
    command = function(target_name, task_name, ...) {
      this_time <- parse_meteo_filepath(task_name, 'time') # better filtering needed?
      this_x <- parse_meteo_filepath(task_name, 'x')
      this_y <- parse_meteo_filepath(task_name, 'y')
      pattern <- sprintf('time\\[%1.0f.%1.0f\\]_x\\[%1.0f\\]_y\\[%1.0f\\]', this_time[1], this_time[2], this_x, this_y)
      these_files <- feather_ind_files[grepl(pattern, feather_ind_files)] %>%
        paste("\n       \'", ., collapse = "\',", sep = "")
      sprintf('feathers_to_driver_file(target_name, dirname = I(\'%s\'), %s\')', data_dir, these_files)
    }
  )

  out_dir <- dirname(driver_files) %>% unique()
  stopifnot(length(out_dir) == 1) # more than one not supported with this pattern
  filenames <- basename(driver_files)

  create_task_plan(filenames, list(driver_task_step), final_steps='munge_drivers', ind_dir = ind_dir)
}

create_driver_task_makefile <- function(makefile, task_plan, include, packages, sources){
  create_task_makefile(
    task_plan, makefile = makefile, ind_complete = TRUE,
    include = include, sources = sources,
    file_extensions=c('ind'), packages = packages)

}


feathers_to_driver_file <- function(filepath, ..., dirname, feather_files = NULL){
  if (is.null(feather_files)){
    feather_inds <- c(...)
    feather_filepaths <- file.path(dirname, scipiper::as_data_file(basename(feather_inds)))
  }
  NLDAS_start <- as.POSIXct('1979-01-01 13:00', tz = "UTC")
  all_time_vec <- seq(NLDAS_start, by = 'hour', to = Sys.time())
  file_times <- parse_meteo_filepath(filepath, 'time') + 1 # not zero indexed
  drivers_in <- data.frame(time = all_time_vec[file_times[1]:file_times[2]], stringsAsFactors = FALSE)
  for (feather_filepath in feather_filepaths){
    message(feather_filepath)
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
    select(time, ShortWave, LongWave, AirTemp, RelHum, WindSpeed, Rain, Snow)

  # now downsample?
  readr::write_csv()
}

# from old MATLAB code:
#    INPUT:   Ta - air temperature  [C]
#             Pa - (optional) pressure [mb]
#
#    OUTPUT:  q  - saturation specific humidity  [kg/kg]
qsat = function(Ta, Pa){
  ew = 6.1121*(1.0007+3.46e-6*Pa)*exp((17.502*Ta)/(240.97+Ta)) # in mb
  q  = 0.62197*(ew/(Pa-0.378*ew))                              # mb -> kg/kg
  return(q)
}
