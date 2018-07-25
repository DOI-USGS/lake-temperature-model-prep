create_feather_task_makefile <- function(makefile, feather_files, include, feather_dir, ind_dir, packages, sources){
  cube_task_step <- create_task_step(
    step_name = 'nccopy',
    target = function(task_name, step_name, ...) {
      file.path(nc_dir, task_name)
    },
    command = "nccopy_nldas(target_name)"
  )
  
  cube_task_plan <- create_task_plan(sub_files, list(cube_task_step), final_steps='nccopy', ind_dir=ind_dir)
  
  create_task_makefile(
    cube_task_plan, makefile = makefile, ind_complete = TRUE, 
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
    mutate(orig_file = create_feather_filename(t0, t1, x, y, variable)) %>% 
    select(-t0, -t1)
  
  return(update_cells)
}

create_feather_filename <- function(t0, t1, x, y, variable, prefix = 'NLDAS', dirname = '2_driver_cells/out'){
  sapply(seq_len(length(t0)), function(i) {
    file.path(dirname, sprintf("%s_time[%s.%s]_x[%s]_y[%s]_var[%s].feather", prefix, t0[i], t1[i], x[i], y[i], variable[i]))
    }, USE.NAMES = FALSE)
}

parse_feather_filename <- function(filename, out = c('var','y','x','time')){
  
  word_indx <- switch(out, 
                      var = 4,
                      y = 3,
                      x = 2, 
                      time = 1)
  gsub("\\[|\\]", "", regmatches(filename, gregexpr("\\[.*?\\]", filename))[[1]][word_indx])
  
}


nc_matrix <- function(filename){
  nc_object <- nc_open(filename)
  nc_data <- ncvar_get(nc_object) # need to index...
  browser()
  attr(nc_data)
  nc_close(nc_object)
  return(nc_data)
}

nc_to_feather <- function(filename, nc_matrix){
  nc_data <- ncvar_get(nc_object, varid = parse_feather_filename(filename, out = 'var'))
  write_feather(data.frame(x = c(2,3,52)), path = filename)
}

append_cell_file <- function(filename){
  # throwaway test...
  data_in <- read_feather(filename)
  data_in$y = c(3,3,2)
  write_feather(data_in, path = filename)
}