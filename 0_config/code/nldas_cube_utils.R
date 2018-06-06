
nldas_cube_diff <- function(new_cube_df, old_cube_df_filename){
  old_cube_df <- readRDS(old_cube_df_filename)
  diffed_cells <- dplyr::anti_join(new_cube_df, old_cube_df,  by = c('x','y')) 
  
  if (any(is.na(diffed_cells))){
    stop('found NA(s) in NLDAS cell diff. Check ', old_cube_df_filename, ' and `new_cube_df` data')
  }
  cube_bounds <- diffed_cells %>% 
    summarize(x0 = min(x), x1 = max(x), y0 = min(y), y1 = max(y))
  
  return(cube_bounds)
}


calc_nldas_files <- function(time_range, time_stride, cube, ...){
  
  space_chunk <- sprintf("%s.%s_%s.%s", cube$x0, cube$x1, cube$y0, cube$y1)
  variables <- c(...)
  if (length(variables) == 0){
    stop('must specify at least one variable as input to ... argument')
  }
  time_chunk_lead <- seq(time_range$t0, time_range$t1, by = time_stride)
  time_chunk_follow <- c(tail(time_chunk_lead, -1L) - 1, time_range$t1)
  
  time_chunks <- sprintf('%s.%s', time_chunk_lead, time_chunk_follow)
  
  nldas_files <- rep(NA_character_, length(time_chunk_lead)*length(variables))
  
  file_i <- 1
  for (variable in variables){
    for (i in 1:length(time_chunks)){
      nldas_files[file_i] <- sprintf("NLDAS_%s_%s_%s.nc", time_chunks[i], space_chunk, variable) #NLDAS_0.9999_132.196_221.344_pressfc.nc
      file_i <- file_i+1
    }
  }
  return(nldas_files)
}