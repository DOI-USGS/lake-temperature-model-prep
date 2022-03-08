#' get elevation values from specific points, using a task plan for a grid of
#' calls from the `elevatr` package
#'
#' @param out_ind the indicator file for the output created by this function
#' @param zoom the zoom level (int) for the data. Different sources are used at
#' different zoom levels, and you can learn more at ?elevatr::get_aws_points by
#' following the links
#' @param points_sf_ind the indicator file for the spatial points file (.rds)
#' that you want elevation values for
#' @param box_res the size, in lat/lon degrees, that you want the tiles to be
#' for the data calls. Each tile is a task, so smaller tiles will create more tasks
#' and less greedy rebuilds if the lakes change, larger tiles will lump more calls
#' into each task
#' @param dummy a character, not used for any reason other than to easily trigger
#' rebuilds when nothing else is changed. We use this here since remake doesn't
#' track the data behind the remote resources, and if we have knowledge that those
#' changed, we can update this value.
#' @param ... filepaths for sources that contain functions used in the task targets
fetch_ned_points <- function(out_ind, zoom, points_sf_ind, box_res, dummy, ...){

  points_sf <- scipiper::sc_retrieve(points_sf_ind) %>% readRDS()

  data_file <- as_data_file(out_ind)

  # make a greedy grid of tiles that all start in the same lower left corner
  # so that they don't shift when we add new lakes
  bbox_grid <- sf::st_make_grid(points_sf, square = TRUE, cellsize = box_res, offset = c(-180,15))
  # which boxes contain points? Only make tasks for those:
  overlapping_boxes <- st_intersects(bbox_grid, points_sf, sparse = F) %>%
    rowSums() %>% as.logical() %>% which()

  # write file of buffers that fit in each box
  # rows are points, columns are boxes. Contents are logicals:
  box_df <- st_within(points_sf, bbox_grid, sparse = F) %>%
    as_tibble(.name_repair = function(x){paste0('ned_bbox_', seq_len(length(x)))})
  # Define task table rows
  tasks <- paste0('ned_bbox_',overlapping_boxes)

  tmp_ned_dir <- 'tmp/ned_pt_sf'
  dir.create(tmp_ned_dir, recursive = TRUE)

  task_files <- tibble(task_name = tasks) %>%
    mutate(filename = sprintf('%s/%s_sf_points.rds', tmp_ned_dir, task_name))

  # write the spatial data for each box to a separate file. This keeps the tiles
  # separate for rebuilds, otherwise all tiles would rebuild if we added a lake
  purrr::map(task_files$filename, function(x){
    taskname <- filter(task_files, filename == x) %>% pull(task_name)
    saveRDS(points_sf[box_df[[taskname]], ], x)
  })

  # Define task table columns
  download_step <- create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      sprintf('%s', task_name)
    },
    command = function(task_name, ...){
      filename <- task_files %>% filter(task_name == !!task_name) %>% pull(filename)
      sprintf("get_ned_points('%s', zoom = %s, dummy = I('%s'))", filename, zoom, dummy)
    }
  )


  base_task_type <- basename(data_file) %>% tools::file_path_sans_ext()
  remakefile <- base_task_type %>% paste0('_tasks.yml')
  sources <- c(...)

  task_plan <- create_task_plan(
    task_names = tasks,
    task_steps = list(download_step),
    final_steps = c('download'),
    add_complete = FALSE)


  # Create the task remakefile
  create_task_makefile(
    task_plan = task_plan,
    makefile = remakefile,
    packages = c('elevatr','dplyr', 'readr', 'sf'),
    sources = sources,
    final_targets = data_file,
    finalize_funs = 'bind_to_feather',
    tickquote_combinee_objects=TRUE,
    as_promises=TRUE)

  loop_tasks(task_plan = task_plan, task_makefile = remakefile, n_cores = 1)

  gd_put(out_ind, data_file)
  file.remove(remakefile)
  unlink(tmp_ned_dir, recursive = TRUE)

}
