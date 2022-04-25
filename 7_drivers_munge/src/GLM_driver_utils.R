write_NLDAS_drivers <- function(out_ind, cell_group_ind, ind_dir){

  driver_task_plan <- create_driver_task_plan(cell_group_ind = cell_group_ind, ind_dir = ind_dir)

  task_remakefile <- '7_drivers_munge_tasks.yml'
  create_driver_task_makefile('7_drivers_munge_tasks.yml', driver_task_plan, final_target = out_ind)

  browser()
  scmake(remake_file = task_remakefile)
  loop_tasks(driver_task_plan, task_makefile = task_remakefile)
  data_file <- as_data_file(out_ind)
  gd_put(out_ind, data_file)
  file.remove(remakefile)

}

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

add_parse_xy_cols <- function(df){
  df %>% rowwise() %>%
    mutate(x = parse_meteo_filepath(filepath, 'x'),
           y = parse_meteo_filepath(filepath, 'y')) %>% ungroup
}

filter_task_files <- function(cell_group_table_ind, box_bounds, meteo_dir){


  out_files <- calc_driver_files(cell_group_table_ind = cell_group_table_ind, meteo_dir)
  xmin <- box_bounds[1]
  xmax <- box_bounds[2]
  ymin <- box_bounds[3]
  ymax <- box_bounds[4]
  box_sf <- st_sfc(st_polygon(x = list(matrix(c(xmin,xmax,xmax, xmin, xmin,
                                                ymin,ymin,ymax,ymax,ymin), ncol = 2))))

  cell_files <- sc_retrieve(cell_group_table_ind) %>% readRDS() %>%
    add_parse_xy_cols() %>%
    rename(featherpath = filepath)
  driver_files <- tibble(filepath = out_files) %>% add_parse_xy_cols() %>%
    rename(driverpath = filepath)

  task_files_sf <- inner_join(cell_files, driver_files, by = c('x','y'))

  cells_sf <- sf::st_as_sf(task_files_sf, coords = c("x", "y"))

  cell_idx <- st_within(cells_sf, box_sf,sparse = F) %>% rowSums() %>% as.logical() %>% which()
  # seems unnecessary because all of these are in the same grouped ind?:
  cells_sf %>% st_drop_geometry() %>%
    slice(cell_idx) %>%
    dplyr::select(driverpath, featherpath, hash) %>% arrange(driverpath, featherpath)
}

#' combine a bunction of .yml files that are the result of `sc_indicate` into
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

create_driver_task_plan <- function(cell_group_ind, ind_dir){

  # smaller to create more tasks, bigger makes the individual tasks take longer
  # and be more at risk to fail
  task_cell_size <- 10

  # hard-coding this, but it doesn't matter if it is wrong because main point is that it is a static grid
  # and we'll error if any points are outside of it
  NLDAS_box <- st_sfc(st_polygon(x = list(matrix(c(0,464,0, 0,224,0), ncol = 2))))
  NLDAS_task_grid <- sf::st_make_grid(NLDAS_box, square = TRUE, cellsize = task_cell_size, offset = c(-0.5,-0.5))
  file_info <- sc_retrieve(cell_group_ind) %>% readRDS() %>%
    rowwise() %>%
    mutate(x = parse_meteo_filepath(filepath, 'x'),
           y = parse_meteo_filepath(filepath, 'y')) %>% ungroup

  cells_sf <- sf::st_as_sf(file_info, coords = c("x", "y"))
  overlapping_boxes <- st_intersects(NLDAS_task_grid, cells_sf, sparse = F) %>% rowSums() %>% as.logical() %>% which()

  # the update we need is to create tasks that are based on the NLDAS task boxes
  # and then filter the output files and input cell files relevant to those.

  # each task is a group of driver outputs as a hash table, those all get run
  # through bind at the end as a combiner and the final result is a huge
  # hash table .ind file


  file_filter_step <- create_task_step(

    step_name = 'filter_files',
    target = function(task_name, step_name, ...) {

      sprintf("box_%s_driver_filepaths", task_name)
    },
    command = function(target_name, task_name, ...) {

      box_indx <- as.numeric(task_name)
      this_bbox <- st_bbox(NLDAS_task_grid[box_indx])
      sprintf('filter_task_files(cell_group_table_ind = \'%s\',
      meteo_dir = I(\'%s\'),
      box_bounds = I(c(%s, %s, %s, %s)))',
              cell_group_ind,
              ind_dir,
              this_bbox[['xmin']], this_bbox[['xmax']], this_bbox[['ymax']], this_bbox[['ymin']])
    }
  )


  #box_7_drivers_out: :
  #   command: feathers_to_driver_files(driver_table = box_7_driver_filepaths)
  driver_task_step <- create_task_step(
    step_name = 'munge_drivers',
    target = function(task_name, step_name, ...) {
      sprintf("box_%s_drivers_out", task_name)
    },
    command = function(target_name, task_name, ...) {
      sub_group_table <- sprintf("box_%s_driver_filepaths", task_name)
      sprintf('feathers_to_driver_files(driver_table = %s)', sub_group_table)
    }
  )

  create_task_plan(as.character(overlapping_boxes),
                   list(file_filter_step, driver_task_step),
                   final_steps='munge_drivers',
                   ind_dir = ind_dir, add_complete = FALSE)
}

create_driver_task_makefile <- function(makefile, task_plan, final_target){
  include <- "7_drivers_munge.yml"
  packages <- c('dplyr', 'feather', 'readr','lubridate', 'sf')
  sources <- '7_drivers_munge/src/GLM_driver_utils.R'

  create_task_makefile(
    task_plan, makefile = makefile,
    include = include, sources = sources,
    finalize_funs = "combine_hash_tables",
    final_targets = final_target,
    file_extensions=c('ind'), packages = packages)

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
    filter(n == 24) %>% rename(time = date) %>% dplyr::select(-n)

  stopifnot(length(unique(diff(drivers_out$time))) == 1)

  readr::write_csv(x = drivers_out, path = filepath)
  return(tools::md5sum(filepath))
}

combine_hash_tables <- function(ind_out, ...){
  hash_table <- bind_rows(...)
  browser()
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


