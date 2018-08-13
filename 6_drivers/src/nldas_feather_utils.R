create_cell_task_plan <- function(cells, time_range, cube_files, cell_data_dir, cell_ind_dir, cube_data_dir, cube_ind_dir){

  cube_task_step <- create_task_step(
    step_name = 'build_feathers',
    target = function(task_name, step_name, ...) {
      file.path(cell_data_dir, task_name)
    },
    command = function(target_name, task_name, ...) {
      this_var <- parse_feather_filename(task_name, 'var') # better filtering needed?
      these_files <- cube_files[grepl(this_var, cube_files)] %>%
        basename() %>% paste0('.ind') %>% file.path(cube_ind_dir, .) %>%
        paste0("\n       '", ., "'", collapse = ",")
      sprintf('cubes_to_cell_file(target_name, nc_dir = I(\'%s\'), %s)', cube_data_dir, these_files)
    }
  )

  cell_filenames <- sapply(seq_len(nrow(cells)), function(j) {
    cell <- cells[j, ]
    create_feather_filename(t0 = time_range[['t0']], t1 = time_range[['t1']], x = cell$x, y = cell$y, variable = cell$variable, dirname = '')
  })
  create_task_plan(cell_filenames, list(cube_task_step), final_steps='build_feathers', ind_dir=cell_ind_dir)
}


create_update_cell_task_plan <- function(cells, time_range, cube_files, cell_data_dir, cell_ind_dir, cube_data_dir, cube_ind_dir, secondary_cube_files = c()){


  cube_files <- c(cube_files, secondary_cube_files)

  time_is_in_range <- function(filepath, times){
    time_range <- parse_nc_filename(filepath, 'time')
    return(all(time_range %in% times))
  }

  cube_task_step <- create_task_step(
    step_name = 'build_feathers',
    target = function(task_name, step_name, ...) {
      file.path(cell_data_dir, task_name)
    },
    command = function(target_name, task_name, ...) {
      src_filepath <- filter(cells, cell_filename == task_name) %>% pull(src_filepath)
      this_var <- parse_feather_filename(task_name, 'var') # better filtering needed?
      this_time <- parse_feather_filename(task_name, 'time')
      that_time <- parse_feather_filename(src_filepath, 'time')
      all_this_time <- this_time[1]:this_time[2]
      all_that_time <- that_time[1]:that_time[2]



      var_files <- cube_files[grepl(this_var, cube_files)] %>%
        basename() %>% paste0('.ind') %>% file.path(cube_ind_dir, .)

      # files in range of the timesteps we want
      time_in_range <- sapply(var_files, time_is_in_range, times = all_this_time, USE.NAMES = FALSE)
      # files that don't have all of their timesteps already covered by the src_filepath file
      time_out_range <- !sapply(var_files, time_is_in_range, times = all_that_time, USE.NAMES = FALSE)
      these_files <- var_files[time_in_range & time_out_range] %>%
        paste0("\n       '", ., "'", collapse = ",")
      sprintf('cubes_to_cell_file(target_name, nc_dir = I(\'%s\'),\n       src_filepath = \'%s\', %s)', cube_data_dir, src_filepath, these_files)
    }
  )

  cell_filename <- sapply(seq_len(nrow(cells)), function(j) {
    cell <- cells[j, ]
    create_feather_filename(t0 = time_range[['t0']], t1 = time_range[['t1']], x = cell$x, y = cell$y, variable = cell$variable, dirname = '')
  })

  cells$cell_filename <- cell_filename
  create_task_plan(cells$cell_filename, list(cube_task_step), final_steps='build_feathers', ind_dir=cell_ind_dir)
}


create_cell_task_makefile <- function(makefile, cell_task_plan){

  include <- "6_drivers_fetch.yml"
  packages <- c('dplyr','ncdf4')
  sources <- '6_drivers/src/nldas_feather_utils.R'

  create_task_makefile(
    cell_task_plan, makefile = makefile, ind_complete = TRUE,
    include = include, sources = sources,
    file_extensions=c('ind'), packages = packages)

}


nldas_diff_cells <- function(new_cells_list, old_cells_df_filename){
  old_cells_df <- readRDS(old_cells_df_filename)

  # expand the list into a data.frame comprable to `old_cells_df`
  new_cells_df <- cell_list_to_df(new_cells_list)

  # leaves only the rows in `new_cells_df` that don't exist in `old_cells_df`
  # these are cell locations where we don't have any data for that variable
  diffed_cells <- dplyr::anti_join(new_cells_df, old_cells_df,  by = c('x','y','variable'))

  return(diffed_cells)
}

#' returns the cells and variable names for the cells that are NOT part of the full/clean subset
nldas_update_cells <- function(new_cells_list, pull_cells, old_times_df_filename){

  old_times_df <- readRDS(old_times_df_filename)

  new_cells_df <- cell_list_to_df(new_cells_list)

  update_cells <- dplyr::anti_join(new_cells_df, pull_cells,  by = c('x','y','variable')) %>%
    left_join(old_times_df, by = c('variable')) %>%
    mutate(src_filepath = create_feather_filename(t0, t1, x, y, variable)) %>%
    select(-t0, -t1)

  return(update_cells)
}

create_feather_filename <- function(t0, t1, x, y, variable, prefix = 'NLDAS', dirname = '6_drivers/out/feather'){
  sapply(seq_len(length(t0)), function(i) {
    if (dirname == ''){
      sprintf("%s_time[%1.0f.%1.0f]_x[%1.0f]_y[%1.0f]_var[%s].feather", prefix, t0[i], t1[i], x[i], y[i], variable[i])
    } else {
      file.path(dirname, sprintf("%s_time[%1.0f.%1.0f]_x[%1.0f]_y[%1.0f]_var[%s].feather", prefix, t0[i], t1[i], x[i], y[i], variable[i]))
    }
    }, USE.NAMES = FALSE)
}

parse_feather_filename <- function(filename, out = c('var','y','x','time')){

  word_indx <- switch(out,
                      var = 4,
                      y = 3,
                      x = 2,
                      time = 1)
  char <- gsub("\\[|\\]", "", regmatches(filename, gregexpr("\\[.*?\\]", filename))[[1]][word_indx])
  if (out == 'var'){
    return(char)
  } else {
    return(as.numeric(strsplit(char, '[.]')[[1]]))
  }
}


#' @param filename the feather file to be written, generated w/ `create_feather_filename`
#' @param ... nc.ind files passed in as unnamed arguments
#' @param nc_dir the directory of nc files (ignored if `nc_files` is not NULL)
#' @param nc_files a vector of nc files (if used, `...` is ignored)
#' @param src_filepath an existing file to use for data
#'
cubes_to_cell_file <- function(filename, ..., nc_dir, nc_files = NULL, src_filepath = NULL){

  cell_x_index <- parse_feather_filename(filename, 'x')
  cell_y_index <- parse_feather_filename(filename, 'y')
  cell_time_range <- parse_feather_filename(filename, 'time')
  cell_time_indices <- seq(cell_time_range[1], cell_time_range[2]) + 1 # our data vectors aren't 0 indexed
  cell_var <- parse_feather_filename(filename, 'var')

  cell_out <- data.frame(data = rep(NA_real_, length(cell_time_indices))) %>% setNames(cell_var)

  if (is.null(nc_files)){
    nc.ind_files <- c(...)
    nc_files <- file.path(nc_dir, scipiper::as_data_file(basename(nc.ind_files)))
  }
  if (!is.null(src_filepath)){
    starter_cell_data <- read_feather(src_filepath)
    file_time_range <- parse_feather_filename(src_filepath, 'time')
    time_indices <- seq(file_time_range[1], file_time_range[2]) + 1
    # src_filepath 'var' better be the same as "cell_var", otherwise this is a NULL replacement:
    cell_out[[cell_var]][time_indices] <- starter_cell_data[[cell_var]]
  }
  for (nc_file in nc_files){
    file_time_range <- parse_nc_filename(nc_file, 'time')
    time_indices <- seq(file_time_range[1], file_time_range[2]) + 1 # our data vectors aren't 0 indexed

    if (any(!time_indices %in% cell_time_indices)){
      stop('attempting to get cube data outside of bounds of cell data in ', nc_file, call. = FALSE)
    }
    # we are ok w/ some overwriting of data if there is a src_filepath, since it should be the same
    cell_out[[cell_var]][time_indices] <- cube_to_cell(nc_file, cell_x_index, cell_y_index, cell_var)
  }
  if(any(is.na(cell_out[[cell_var]]))){
    stop('cell has NA values after extracting data from cubes', call. = FALSE)
  }

  feather::write_feather(cell_out, filename)
  invisible(filename)
}

# dumps all time for now...no time arg
cube_to_cell <- function(cube_file, x_index, y_index, var){
  cube_x_range <- parse_nc_filename(cube_file, 'x')
  cube_y_range <- parse_nc_filename(cube_file, 'y')
  cube_var <- parse_nc_filename(cube_file, 'var')

  stopifnot(x_index %in% seq(cube_x_range[1], cube_x_range[2]))
  stopifnot(y_index %in% seq(cube_y_range[1], cube_y_range[2]))
  stopifnot(var == cube_var)

  x_start <- x_index - cube_x_range[1] + 1 # our data vectors aren't 0 indexed
  y_start <- y_index - cube_y_range[1] + 1 # our data vectors aren't 0 indexed

  nc <- nc_open(cube_file, suppress_dimvals = TRUE)
  cell_data <- ncvar_get(nc, varid = var, start = c(x_start, y_start, 1), count = c(1, 1, -1))
  nc_close(nc)

  return(cell_data)
}


append_cell_file <- function(filename, ...){
  stop('not implemented')

  data_in <- read_feather(filename) # old file name vs new?

  stop('not implemented')
  write_feather(data_in, path = filename)
}
