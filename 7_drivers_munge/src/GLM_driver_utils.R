

#' from the comprehensive hash table of all driver cell feather files
#' (these files are in `cell_group_table_file`), filter out the section of files
#' that pertains to the area within `box_bounds`, which is a x,y indexed bounding
#' box. For each cell, create a output driver filepath
#'
#' @param cell_group_table_file an .rds file containing `filepath` and `hash` for
#' feather files
#' @param box_bounds a vector of length 4 containing xmin/max, ymin/max
#' @param meteo_dir the directory to use for driver filepaths that are created
#'
#' @returns a hash table with `driverpath`, `featherpath`, and `hash`
filter_task_files <- function(cell_group_table_file, box_bounds, meteo_dir){

  xmin <- box_bounds[1]
  xmax <- box_bounds[2]
  ymin <- box_bounds[3]
  ymax <- box_bounds[4]

  cell_files <- readRDS(cell_group_table_file) %>%
    # a faster alternative to `parse_meteo_filepath()` is to use this regex:
    mutate(x = stringr::str_extract(filepath, '(?<=x\\[).+?(?=\\])'),
           y = stringr::str_extract(filepath, '(?<=y\\[).+?(?=\\])'),
           time = stringr::str_extract(filepath, '(?<=time\\[).+?(?=\\])'),
           x_num = as.numeric(x), y_num = as.numeric(y)) %>%
    filter(x_num > xmin & x_num < xmax & y_num > ymin & y_num < ymax) %>%
    rename(featherpath = filepath)

  driver_files <- cell_files %>% dplyr::select(time, x, y, x_num, y_num) %>%
    distinct() %>%
    # split up the `time` format into two columns so we can use our shared meteo_filepath function:
    tidyr::extract(time, '([0-9]+).([0-9]+)', into = c('t0','t1'),
                   remove = FALSE, convert = TRUE) %>%
    mutate(driverpath = create_meteo_filepath(t0, t1, x_num, y_num, prefix = 'NLDAS', dirname = meteo_dir)) %>%
    dplyr::select(driverpath, x, y, time)

  # join filtered driver files to filtered feather files, which expands/repeats
  # driver files given each .csv has a .feather for each variable:
  inner_join(cell_files, driver_files, by = c('x','y', 'time')) %>%
    dplyr::select(driverpath, featherpath, hash) %>%
    # sort so that we get a reliable hash table that doesn't register as stale
    # for arbitrary reasons (e.g., "order"):
    arrange(driverpath, featherpath) %>%
    as_tibble()
}

#' combine a bunch of .yml files that are the result of `sc_indicate` into
#'   a single file
merge_cell_group_files <- function(ind_out, cell_group_ind){

  # create a table of
  # filepath                             hash
  # /Volumes/ThunderBlade/NLDAS_feather//NLDAS_time[0.359420]_x[220]_y[192]_var[dlwrfsfc].feather ab41a808b4517ccab80a4ddc675928d2
  cell_group_ymls <- as_data_file(names(yaml::yaml.load_file(cell_group_ind)))

  file_list <- lapply(cell_group_ymls, function(x){
    contents <- yaml::read_yaml(x) %>% unlist
    hashes <- unname(contents)
    filepaths <- names(contents)
    data.frame(filepath = filepaths, hash = hashes)
  })

  file_table <- do.call(rbind, file_list)
  data_file <- as_data_file(ind_out)
  saveRDS(file_table, file = data_file)

  gd_put(ind_out)
}


#' shared function for creating consistent references to the driver files
create_meteo_filepath <- function(t0, t1, x, y, prefix = 'NLDAS', dirname){
  file.path(dirname, sprintf("%s_time[%1.0f.%1.0f]_x[%1.0f]_y[%1.0f].csv", prefix, t0, t1, x, y))
}

parse_meteo_filepath <- function(filepath, out = c('y','x','time')){
  # this is slow (not vectorized), should use str_extract(filepath, '(?<={out}\\[).+?(?=\\])') instead
  word_indx <- switch(out,
                      y = 3,
                      x = 2,
                      time = 1)
  char <- gsub("\\[|\\]", "", regmatches(filepath, gregexpr("\\[.*?\\]", filepath))[[1]][word_indx])
  return(as.numeric(strsplit(char, '[.]')[[1]]))
}



feathers_to_driver_files <- function(driver_table){
  driver_table %>% group_by(driverpath) %>%
    summarize(hash = feathers_to_driver_file(driverpath[1], featherpath))
}

#' convert a bunch of variable-specific feather files into a single driver file
#'
#' @param filepath the output file to use
#' @param cell_group_table a data.frame with `filepath` and `hash`, where `filepath`
#'    points to the location of feather files to use for each cell
feathers_to_driver_file <- function(filepath, feather_filepaths){


  NLDAS_start <- as.POSIXct('1979-01-01 13:00', tz = "UTC")
  all_time_vec <- seq(NLDAS_start, by = 'hour', to = Sys.time())
  file_times <- parse_meteo_filepath(filepath, 'time') + 1 # not zero indexed
  drivers_in <- data.frame(time = all_time_vec[file_times[1]:file_times[2]], stringsAsFactors = FALSE)
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
    dplyr::select(time, ShortWave, LongWave, AirTemp, RelHum, WindSpeed, Rain, Snow) %>% # now downsample?
    mutate(date = lubridate::as_date(time)) %>% group_by(date) %>%
    summarize(ShortWave = mean(ShortWave), LongWave = mean(LongWave),
              AirTemp = mean(AirTemp), RelHum = mean(RelHum),
              WindSpeed = mean(WindSpeed^3)^(1/3), Rain = mean(Rain), Snow = mean(Snow), n = length(time)) %>%
    # odd GLM edge-case when ice is forming on a timestep that has _both_ rain and snow
    # causing issues. Avoid by only
    mutate(Rain = case_when(Snow > 0 ~ 0, TRUE ~ Rain)) %>%
    filter(n == 24) %>% rename(time = date) %>% dplyr::select(-n)

  stopifnot(length(unique(diff(drivers_out$time))) == 1)

  readr::write_csv(x = drivers_out, path = filepath)
  return(tools::md5sum(filepath))
}

combine_hash_tables <- function(ind_out, ...){
  hash_table <- bind_rows(...)
  data_file <- as_data_file(ind_out)

  arrow::write_feather(x = hash_table, sink = data_file)
  gd_put(ind_out, data_file)
}

index_local_drivers <- function(ind_out, depends){

  a_build_index <- yaml::yaml.load_file(depends) %>% names

  # because all files in the "depends" come from the same replete_time_range,
  # they have to have the same time index, hence we'll use the first:
  time_range <- parse_meteo_filepath(a_build_index[1], 'time')
  driver_dir <- dirname(a_build_index) %>% unique()

  stopifnot(length(driver_dir) == 1)

  data_file <- scipiper::as_data_file(ind_out)

  tibble(local_driver = dir(driver_dir)) %>%
    filter(str_detect(local_driver, sprintf('^NLDAS_time\\[%s.%s\\]', time_range[1], time_range[2]))) %>%
    saveRDS(file = data_file)

  gd_put(ind_out)
}

# from old MATLAB code:
#' @param Ta - air temperature  [C]
#' @param Pa - pressure [mb]
#'
#' @return  q  - saturation specific humidity  [kg/kg]
qsat <- function(Ta, Pa){
  ew <- 6.1121*(1.0007+3.46e-6*Pa)*exp((17.502*Ta)/(240.97+Ta)) # in mb
  q <- 0.62197*(ew/(Pa-0.378*ew))                              # mb -> kg/kg
  return(q)
}


