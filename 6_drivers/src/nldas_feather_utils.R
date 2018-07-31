create_cell_task_plan <- function(cells, time_range, cube_files, ind_dir, feather_dir){

  cube_task_step <- create_task_step(
    step_name = 'build_feathers',
    target = function(task_name, step_name, ...) {
      file.path(feather_dir, task_name)
    },
    command = function(target_name, task_name, ...) {
      this_var <- parse_feather_filename(task_name, 'var') # better filtering needed?
      these_files <- cube_files[grepl(this_var, cube_files)] %>%
        paste("\n       \'", ., collapse = "\',", sep = "")
      sprintf('cubes_to_cell_file(target_name, %s\')', these_files)
    }
  )

  cell_filenames <- sapply(seq_len(nrow(cells)), function(j) {
    cell <- cells[j, ]
    create_feather_filename(t0 = time_range[['t0']], t1 = time_range[['t1']], x = cell$x, y = cell$y, variable = cell$variable, dirname = '')
  })
  create_task_plan(cell_filenames, list(cube_task_step), final_steps='build_feathers', ind_dir=ind_dir)
}


create_feather_task_makefile <- function(makefile, cell_task_plan, include, packages, sources){
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
    mutate(src_filename = create_feather_filename(t0, t1, x, y, variable)) %>%
    select(-t0, -t1)

  return(update_cells)
}

create_feather_filename <- function(t0, t1, x, y, variable, prefix = 'NLDAS', dirname = '8_drivers_munge/out'){
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
#' @param ... nc files passed in as unnamed arguments
#' @param nc_files a vector of nc files (if used, `...`` is ignored)
#'
cubes_to_cell_file <- function(filename, ..., nc_files = NULL){

  cell_x_index <- parse_feather_filename(filename, 'x')
  cell_y_index <- parse_feather_filename(filename, 'y')
  cell_time_range <- parse_feather_filename(filename, 'time')
  cell_time_indices <- seq(cell_time_range[1], cell_time_range[2]) + 1 # our data vectors aren't 0 indexed
  cell_var <- parse_feather_filename(filename, 'var')

  cell_out <- data.frame(data = rep(NA_real_, length(cell_time_indices))) %>% setNames(cell_var)

  if (is.null(nc_files)){
    nc_files <- c(...)
  }

  for (nc_file in nc_files){
    file_time_range <- parse_nc_filename(nc_file, 'time')
    time_indices <- seq(file_time_range[1], file_time_range[2]) + 1 # our data vectors aren't 0 indexed

    if (any(!time_indices %in% cell_time_indices)){
      stop('attempting to get cube data outside of bounds of cell data in ', nc_file, call. = FALSE)
    }

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

  x_start <- cube_x_range[1]-x_index + 1 # our data vectors aren't 0 indexed
  y_start <- cube_y_range[1]-y_index + 1 # our data vectors aren't 0 indexed

  nc <- nc_open(cube_file, suppress_dimvals = TRUE)
  cell_data <- ncvar_get(nc, varid = var, start = c(cell.x[1], cell.y[1], 1), count = c(1, 1, -1))
  nc_close(nc)

  return(cell_data)
}


append_cell_file <- function(filename, ...){
  stop('not implemented')

  data_in <- read_feather(filename) # old file name vs new?

  stop('not implemented')
  write_feather(data_in, path = filename)
}
