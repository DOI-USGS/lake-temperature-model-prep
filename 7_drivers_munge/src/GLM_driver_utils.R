calc_feather_ind_files <- function(grid_cells, time_range, ind_dir){
  cell_list_to_df(grid_cells) %>%
    mutate(t0 = time_range[1], t1 = time_range[2]) %>%
    mutate(filepath = paste0(create_feather_filename(t0, t1, x = x, y = y, variable = variable, dirname = ind_dir), '.ind')) %>%
    pull(filepath)
}

calc_driver_files <- function(cell_group_table_ind, dirname){
  cell_group_table <- sc_retrieve(cell_group_table_ind) %>% readRDS
  feather_files <- cell_group_table$filepath
  driver_files <- rep(NA_character_, length(feather_files))
  for (i in seq_len(length(feather_files))){
    feat_file <- feather_files[i]
    times <- parse_feather_filename(feat_file, out = 'time')
    x <- parse_feather_filename(feat_file, out = 'x')
    y <- parse_feather_filename(feat_file, out = 'y')
    driver_files[i] <- create_meteo_filepath(times[1], times[2], x, y, dirname = dirname)
  }
  return(unique(driver_files))
}

#' #' combine a bunction of .yml files that are the result of `sc_indicate` into
#' #'   a single file
#' merge_cell_group_files <- function(cell_group_files){
#'   file_list <- lapply(cell_group_files$filename, function(x){
#'     contents <- yaml::read_yaml(x) %>% unlist
#'     hashes <- unname(contents)
#'     filepaths <- names(contents)
#'     data.frame(filepath = filepaths, hash = hashes)
#'   })
#'   return(do.call(rbind, file_list))
#' }
#'
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

filter_cell_group_table <- function(cell_group_table, pattern){
  filter(cell_group_table, grepl(pattern = pattern, x = filepath))
}

retrieve_and_readRDS <- function(in_ind){
  sc_retrieve(in_ind) %>% readRDS
}

create_driver_task_plan <- function(driver_files, cell_group_table, ind_dir){

  # now use table...was ind_files

  # we want to get the name of the object passed to `cell_group_table`, so we can refer to it elsewhere
  cl <- sys.call(0)
  f <- get(as.character(cl[[1]]), mode="function", sys.frame(-1))
  cl <- match.call(definition=f, call=cl)
  # **perhaps we should be using an .rds or .feather file here so we don't need to do this goofy arg-grabbing?**
  cell_group_obj <- as.list(cl)[-1][['cell_group_table']] %>% as.character()

  as_sub_group_table <- function(filename){
    gsub(pattern = '\\[', '_', x = tools::file_path_sans_ext(filename)) %>%
      gsub(pattern = '\\]', '', x = .) %>%
      paste0("_cell_group_table")
  }
  subset_table_task_step <- create_task_step(
    step_name = 'subset_table',
    target = function(task_name, step_name, ...) {
      as_sub_group_table(task_name)
    },
    command = function(target_name, task_name, ...) {
      this_time <- parse_meteo_filepath(task_name, 'time') # better filtering needed?
      this_x <- parse_meteo_filepath(task_name, 'x')
      this_y <- parse_meteo_filepath(task_name, 'y')
      # need to double escape to preserve the "\\"
      pattern <- sprintf('time\\\\[%1.0f.%1.0f\\\\]_x\\\\[%1.0f\\\\]_y\\\\[%1.0f\\\\]', this_time[1], this_time[2], this_x, this_y)
      sprintf('filter_cell_group_table(%s, pattern = I(\'%s\'))', cell_group_obj, pattern)
    }
  )

  driver_task_step <- create_task_step(
    step_name = 'munge_drivers',
    target = function(task_name, step_name, ...) {
      file.path(out_dir, task_name)
    },
    command = function(target_name, task_name, ...) {
      sub_group_table <- as_sub_group_table(task_name)
      sprintf('feathers_to_driver_file(target_name, cell_group_table = %s)', sub_group_table)
    }
  )

  out_dir <- dirname(driver_files) %>% unique()
  stopifnot(length(out_dir) == 1) # more than one not supported with this pattern
  filenames <- basename(driver_files)

  create_task_plan(filenames, list(subset_table_task_step, driver_task_step), final_steps='munge_drivers', ind_dir = ind_dir, add_complete = FALSE)
}

create_driver_task_makefile <- function(makefile, task_plan){
  include <- "7_drivers_munge.yml"
  packages <- c('dplyr', 'feather', 'readr','lubridate')
  sources <- '7_drivers_munge/src/GLM_driver_utils.R'

  create_task_makefile(
    task_plan, makefile = makefile,
    include = include, sources = sources,
    file_extensions=c('ind'), packages = packages)

}


#' convert a bunch of variable-specific feather files into a single driver file
#'
#' @param filepath the output file to use
#' @param cell_group_table a data.frame with `filepath` and `hash`, where `filepath`
#'    points to the location of feather files to use for each cell
feathers_to_driver_file <- function(filepath, cell_group_table){

  feather_filepaths <- cell_group_table$filepath

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
    filter(n == 24) %>% rename(time = date) %>% dplyr::select(-n)

  stopifnot(length(unique(diff(drivers_out$time))) == 1)

  readr::write_csv(x = drivers_out, path = filepath)
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
