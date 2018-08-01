
cell_list_to_df <- function(cells_list){
  data.frame(x = rep(cells_list$x, length(cells_list$variables)),
             y = rep(cells_list$y, length(cells_list$variables)),
             variable = c(sapply(cells_list$variables, function(x) rep(x, length(cells_list$x)))),
             stringsAsFactors = FALSE)
}


#' returns the spatial box and variable names for the cells that are need full/clean data
nldas_diff_box <- function(new_cells_list, old_cells_df_filename){

  diffed_cells <- nldas_diff_cells(new_cells_list, old_cells_df_filename)

  if (any(is.na(diffed_cells))){
    # shouldn't be any NAs, but if there are, throw an error
    stop('found NA(s) in NLDAS cell diff. Check ', old_cells_df_filename, ' and `new_cells_df` data')
  }

  # convert this x,y,variable data.frame into a bounding box data.frame
  box_bounds <- as_box_bounds(diffed_cells)

  return(box_bounds)
}

#' returns the spatial box and variable names for the cells that are NOT part of the full/clean subset
nldas_update_box <- function(new_cells_list, subset_box){

  box_bounds <- cell_list_to_df(new_cells_list) %>%
    mutate(update_cell = out_of_box(x, y, variable, subset_box)) %>%
    filter(update_cell) %>% select(-update_cell) %>%
    as_box_bounds()

  return(box_bounds)
}

as_box_bounds <- function(cells_df){
  cells_df %>% group_by(variable) %>%
    summarize(x0 = min(x), x1 = max(x), y0 = min(y), y1 = max(y))
}

out_of_box <- function(x, y, var, boxes){
  out_logical <- rep(FALSE, length(x))
  # possible to vectorize easily? worth it?
  for (j in seq_len(length(x))){
    box <- boxes[boxes$variable == var[j], ]
    if (nrow(box) != 0) { # zero rows is case where there is no subset box for this variable
      out_logical[j] <- (x[j] < box$x0 | x[j] > box$x1) & (y[j] < box$y0 | y[j] > box$y1)
    }
  }
  return(out_logical)
}



#' @param new_times_range a vector of integers refering to max and min time indices (0 referenced) needed
#' @param old_times_df_filename a filename for .rds holding time indices of _completed_/_processed_ data
#' as a data.frame w/ t0 and t1 indices and variable names
#'
#' @return named vector for `t0` and `t1`, which include the min and max time index needed to cover the time
#' dimension completely
#'
#' @details in the future this could return variable-specific ranges...
nldas_times_diff <- function(new_times_range, old_times_df_filename){
  old_times_df <- readRDS(old_times_df_filename)

  # smallest time range that is completely covered by all of the existing variables:
  old_times_range <- c(t0 = max(old_times_df$t0), t1 = min(old_times_df$t1)) # could be variable-specific here...

  append_time_range <- c(t0 = NA_integer_, t1 = NA_integer_)
  if (old_times_range[["t0"]] > new_times_range[["t0"]] & old_times_range[["t1"]] < new_times_range[["t1"]]){
    # if old times are in the middle of new times, use new times:
    append_time_range <- new_times_range
  } else if (old_times_range[["t1"]] <= new_times_range[["t0"]] | old_times_range[["t0"]] >= new_times_range[["t1"]]) {
    # if old times are earlier or later than new times, use new times:
    append_time_range <- new_times_range
  } else if (old_times_range[["t1"]] >= new_times_range[["t1"]]){
    # if old times have late data that we need, stop times earlier:
    append_time_range[["t1"]] <- old_times_range[["t0"]] - 1
    append_time_range[["t0"]] <- new_times_range[["t0"]]
  } else if (old_times_range[["t0"]] <= new_times_range[["t0"]]){
    # if old times have early data that we need, start times later:
    append_time_range[["t0"]] <- old_times_range[["t1"]] + 1
    append_time_range[["t1"]] <- new_times_range[["t1"]]
  } else {
    stop('this case is not covered...does it exist?')
  }

  stopifnot(diff(append_time_range) >= 0)
  return(append_time_range)
}

#' build a file list according to spatial domain `boxes`, time domain according to `time_range`,
#' and file chunk size according to `time_chunk`
calc_nldas_files <- function(boxes, time_range, time_chunk, nc_dir){

  time_chunk_lead <- seq(time_range[["t0"]], time_range[["t1"]], by = time_chunk)
  time_chunk_follow <- c(tail(time_chunk_lead, -1L) - 1, time_range[["t1"]])

  variables <- boxes$variable
  nldas_files <- rep(NA_character_, length(time_chunk_lead)*length(variables))
  file_i <- 1

  for (variable in variables){
    box <- boxes[boxes$variable == variable, ]
    for (i in 1:length(time_chunk_lead)){
      nldas_files[file_i] <- create_nc_filename(t0 = time_chunk_lead[i], t1 = time_chunk_follow[i],
                                                x0 = box$x0, x1 = box$x1, y0 = box$y0, y1 = box$y1, variable = variable)
      file_i <- file_i+1
    }
  }
  return(file.path(nc_dir, nldas_files))
}

# create file like this: #NLDAS_time[0.9999]_y[132.196]_x[221.344]_var[pressfc].nc
create_nc_filename <- function(t0, t1, x0, x1, y0, y1, variable, prefix = 'NLDAS'){
  stopifnot(t1 < 1000000) # we'd need to change our number padding logic
  t0 <- stringr::str_pad(sprintf("%1.0f", t0), width = 6, pad = '0')
  t1 <- stringr::str_pad(sprintf("%1.0f", t1), width = 6, pad = '0')
  sprintf("%s_time[%s.%s]_x[%1.0f.%1.0f]_y[%1.0f.%1.0f]_var[%s].nc", prefix, t0, t1, x0, x1, y0, y1, variable)
}


parse_nc_filename <- function(filename, out = c('y','x','time','var')){

  filename <- basename(filename)
  word_indx <- switch(out,
                      y = 3,
                      x = 2,
                      time = 1,
                      var = 4)
  char <- gsub("\\[|\\]", "", regmatches(filename, gregexpr("\\[.*?\\]", filename))[[1]][word_indx])
  if (out == 'var'){
    return(char)
  } else {
    return(as.numeric(strsplit(char, '[.]')[[1]]))
  }
}

create_cube_task_plan <- function(sub_files, ind_dir){
  nc_dir <- dirname(sub_files) %>% unique()
  if (length(nc_dir) != 1 & length(sub_files) > 0){
    stop('using more than one dir is not supported for this function currently', call. = FALSE)
  }
  cube_task_step <- create_task_step(
    step_name = 'nccopy',
    target = function(task_name, step_name, ...) {
      file.path(nc_dir, task_name)
    },
    command = "nccopy_split_combine(target_name, max_steps = I(100))"
  )

  cube_task_plan <- create_task_plan(basename(sub_files), list(cube_task_step), final_steps='nccopy', ind_dir=ind_dir)
}
create_cube_task_makefile <- function(makefile, cube_task_plan, include, packages, sources){
  create_task_makefile(
    cube_task_plan, makefile = makefile, ind_complete = TRUE,
    include = include, sources = sources,
    file_extensions=c('ind'), packages = packages)

}
