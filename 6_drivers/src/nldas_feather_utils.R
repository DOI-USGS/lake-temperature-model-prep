create_new_cell_task_plan <- function(cells, cube_files_ind, cell_data_dir, cell_ind_dir){

  # from https://stackoverflow.com/questions/17256834/getting-the-arguments-of-a-parent-function-in-r-with-names
  # we want to get the name of the object passed to `cells`, so we can refer to it elsewhere
  cl <- sys.call(0)
  f <- get(as.character(cl[[1]]), mode="function", sys.frame(-1))
  cl <- match.call(definition=f, call=cl)
  cell_obj <- as.list(cl)[-1][['cells']] %>% as.character()

  # for each of the variable and regional groups, create a task to write the feather files
  # use a data.frame of region and var (repeated)
  # filter first step to just the cells we want

  # hard-coding this, but it doesn't matter if it is wrong because main point is that it is a static grid
  # and we'll error if any points are outside of it
  NLDAS_box <- st_sfc(st_polygon(x = list(matrix(c(0,464,0, 0,224,0), ncol = 2))))
  NLDAS_task_grid <- sf::st_make_grid(NLDAS_box, square = TRUE, cellsize = 100, offset = c(-0.5,-0.5))

  cells_sf <- sf::st_as_sf(cells, coords = c("x", "y"))
  overlapping_boxes <- st_intersects(NLDAS_task_grid, cells_sf, sparse = F) %>% rowSums() %>% as.logical() %>% which()

  cube_files <- yaml.load_file(cube_files_ind) %>% names()
  cube_info <- tibble(filepath = cube_files) %>% rowwise() %>%
    mutate(t0 = parse_cellgroup_filename(filepath, 'time')[1],
           t1 = parse_cellgroup_filename(filepath, 'time')[2])

  # get the info for the min and max time index from the file info:
  time_range <- list(t0 = min(cube_info$t0), t1 = max(cube_info$t1))
  cell_filter_step <- create_task_step(

    step_name = 'filter_cells',
    target = function(task_name, step_name, ...) {
      box_indx <- strsplit(task_name, '_')[[1]][2]
      # need the name to include the filter stuff
      this_var <- parse_cellgroup_filename(task_name, 'var')
      sprintf("box_%s_%s_cells", box_indx, this_var)
    },
    command = function(target_name, task_name, ...) {
      box_indx <- strsplit(task_name, '_')[[1]][2] %>% as.numeric()
      this_bbox <- st_bbox(NLDAS_task_grid[box_indx])
      sprintf('filter_task_cells(cells = %s, box_bounds = I(c(%s, %s, %s, %s)), var = I(\'%s\'))', cell_obj,
              this_bbox[['xmin']], this_bbox[['xmax']], this_bbox[['ymax']], this_bbox[['ymin']],
              parse_cellgroup_filename(task_name, 'var'))
    }
  )

  cube_task_step <- create_task_step(
    step_name = 'build_feathers',
    target = function(task_name, step_name, ...) {
      file.path(cell_ind_dir, task_name)
    },
    command = function(target_name, task_name, ...) {
      this_var <- parse_cellgroup_filename(task_name, 'var') # better filtering needed?
      box_indx <- strsplit(task_name, '_')[[1]][2]
      cell_obj_name <- sprintf("box_%s_%s_cells", box_indx, this_var)
      these_files <- cube_files[grepl(this_var, cube_files)] %>%
        paste0("\n       '", ., "'", collapse = ",")
      # cells = the data.frame object of cells to use
      # out_dir = the directory to write the feather files
      # ... = a vector of .nc data files
      sprintf('cubes_to_cell_files(target_name, cells = %s, out_dir = I(\'%s\'), %s)', cell_obj_name, cell_data_dir, these_files)
    }
  )

  variable_indicators <- sapply(unique(cells$variable), function(x) {
    create_cellgroup_filename(t0 = time_range[['t0']], t1 = time_range[['t1']], variable = x, dirname = "")
  }, USE.NAMES = FALSE)

  tasks <- rep(variable_indicators, length(overlapping_boxes)) %>% sort %>% paste('box', overlapping_boxes, ., sep = '_')
  create_task_plan(tasks, list(cell_filter_step, cube_task_step), final_steps='build_feathers', ind_dir=cell_ind_dir)
}

filter_task_cells <- function(cells, box_bounds, var){
  xmin <- box_bounds[1]
  xmax <- box_bounds[2]
  ymin <- box_bounds[3]
  ymax <- box_bounds[4]
  box_sf <- st_sfc(st_polygon(x = list(matrix(c(xmin,xmax,xmax, xmin, xmin,
                                                ymin,ymin,ymax,ymax,ymin), ncol = 2))))
  cells_sf <- sf::st_as_sf(cells, coords = c("x", "y"))
  cell_idx <- st_within(cells_sf, box_sf,sparse = F) %>% rowSums() %>% as.logical() %>% which()
  cells[cell_idx,] %>% filter(variable == var)
}

create_cellgroup_filename <- function(t0, t1, variable, dirname, prefix = 'cellgroup', ext = '.yml'){
  filename <- sprintf("%s_time[%1.0f.%1.0f]_var[%s]%s", prefix, t0, t1, variable, ext)
  if (dirname != ''){
    filename <- file.path(dirname, filename)
  }
  return(filename)
}
parse_cellgroup_filename <- function(filename, out = c('var','time')){

  word_indx <- switch(out,
                      var = 2,
                      time = 1)
  char <- gsub("\\[|\\]", "", regmatches(filename, gregexpr("\\[.*?\\]", filename))[[1]][word_indx])
  if (out == 'var'){
    return(char)
  } else {
    return(as.numeric(strsplit(char, '[.]')[[1]]))
  }
}


create_cell_task_makefile <- function(makefile, cell_task_plan){

  include <- "6_drivers_fetch.yml"
  packages <- c('dplyr','ncdf4')
  sources <- '6_drivers/src/nldas_feather_utils.R'

  create_task_makefile(
    cell_task_plan, makefile = makefile,
    include = include, sources = sources,
    file_extensions=c('ind'), packages = packages)

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
#' @param ... nc files passed in as unnamed arguments
#' @param nc_dir the directory of nc files (ignored if `nc_files` is not NULL)
#' @param cells a data.frame of cells to use (not necessarily filtered)
#' @param nc_files a vector of nc files (if used, `...` is ignored)
#' @param src_filepath an existing file to use for data
#'
cubes_to_cell_files <- function(filename, ..., cells, out_dir, nc_files = NULL, src_filepath = NULL){


  cell_time_range <- parse_cellgroup_filename(filename, 'time')
  cell_time_indices <- seq(cell_time_range[1], cell_time_range[2]) + 1 # our data vectors aren't 0 indexed
  cell_var <- parse_cellgroup_filename(filename, 'var')
  file_info <- cells %>% filter(variable == cell_var) %>%
    mutate(t0 = cell_time_range[1], t1 = cell_time_range[2],
           filepath = create_feather_filename(t0 = t0, t1 = t1, x = x, y = y, variable = variable, dirname = out_dir))

  # pre-populate huge list that is keyed in by feather filenames:
  sparse_nc_list <- lapply(seq_len(nrow(file_info)), function(x){
    rep(NA_real_, length(cell_time_indices))
  }) %>% setNames(file_info$filepath)

  if (is.null(nc_files)){
    nc_files <- c(...)
  }

  if (!is.null(src_filepath)){
    stop('haven\'t updated this yet')
    starter_cell_data <- read_feather(src_filepath)
    file_time_range <- parse_feather_filename(src_filepath, 'time')
    time_indices <- seq(file_time_range[1], file_time_range[2]) + 1
    # src_filepath 'var' better be the same as "cell_var", otherwise this is a NULL replacement:
    cell_out[[cell_var]][time_indices] <- starter_cell_data[[cell_var]]
  }

  min_x_cell <- min(file_info$x)
  range_x_cell <- diff(range(file_info$x))
  min_y_cell <- min(file_info$y)
  range_y_cell <- diff(range(file_info$y))
  for (nc_file in nc_files){
    file_time_range <- parse_nc_filename(nc_file, 'time')
    # the time values in this .nc file:
    time_indices <- seq(file_time_range[1], file_time_range[2]) + 1 # our data vectors aren't 0 indexed

    if (any(!time_indices %in% cell_time_indices)){
      # fail if the cube has any _extra_ data that we're not using in the cell files
      stop('attempting to get cube data outside of bounds of cell data in ', nc_file, call. = FALSE)
    }
    # + 1 because ncvar_get is not zero indexed
    cube_x_vals <- parse_nc_filename(nc_file, 'x') %>% {seq(.[1], .[2])}
    cube_y_vals <- parse_nc_filename(nc_file, 'y') %>% {seq(.[1], .[2])}
    # start _is_ zero indexed
    x_start <- which(cube_x_vals == min_x_cell)
    y_start <- which(cube_y_vals == min_y_cell)
    x_cnt <- range_x_cell + 1
    y_cnt <- range_y_cell + 1
    # get all of the data from the cube, extract
    nc <- nc_open(nc_file, suppress_dimvals = TRUE)

    cell_data <- ncvar_get(nc, varid = cell_var,
                           start = c(x_start, y_start, 1L),
                           count = c(x_cnt, y_cnt, -1L))
    nc_close(nc)

    # necessary because not zero indexed; this is the subset we grabbed:
    these_y_vals <- seq(cube_y_vals[y_start], length.out = y_cnt)
    these_x_vals <- seq(cube_x_vals[x_start], length.out = x_cnt)
    # we are ok w/ some overwriting of data if there is a src_filepath, since it should be the same

    for (feather_file in file_info$filepath){
      # need to do a spot check on lon/lat vs meteo
      x_index <- parse_feather_filename(feather_file, 'x')
      y_index <- parse_feather_filename(feather_file, 'y')

      # fail fast if the data we're looking for isn't here:
      stopifnot(x_index %in% these_x_vals)
      stopifnot(y_index %in% these_y_vals)

      # grabbing the data by index is _relative_ to the subset
      x_cell_start <- which(x_index == these_x_vals)
      y_cell_start <- which(y_index == these_y_vals)
      sparse_nc_list[[feather_file]][time_indices] <- cell_data[x_cell_start, y_cell_start, ]
    }
  }
  skip_write <- c()
  for (feather_file in file_info$filepath){
    data_out <- sparse_nc_list[[feather_file]]
    if (all(is.na(data_out))){
      # this cell is masked in the domain or missing all data
      skip_write <- c(skip_write, feather_file)
    } else if (any(is.na(data_out))){
      stop('cell has NA values after extracting data from cubes', call. = FALSE)
    } else {
      cell_out <- data.frame(x = data_out) %>% setNames(cell_var)
      feather::write_feather(cell_out, feather_file)
    }
  }
  write_files <- filter(file_info, !filepath %in% skip_write) %>% pull(filepath)
  sc_indicate(filename, data_file = write_files)
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

