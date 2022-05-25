write_NLDAS_drivers <- function(out_ind, cell_group_ind, ind_dir, ...){

  sc_retrieve(cell_group_ind)
  cell_group_file <- as_data_file(cell_group_ind)
  driver_task_plan <- create_driver_task_plan(cell_group_file = cell_group_file, ind_dir = ind_dir)

  task_remakefile <- '7_drivers_munge_tasks.yml'
  create_driver_task_makefile('7_drivers_munge_tasks.yml', driver_task_plan, final_target = out_ind, ...)

  scmake(remake_file = task_remakefile)

  data_file <- as_data_file(out_ind)
  gd_put(out_ind, data_file)
  file.remove(task_remakefile)

}

create_driver_task_plan <- function(cell_group_file, ind_dir){

  # smaller to create more tasks, bigger makes the individual tasks take longer
  # and be more at risk to fail
  task_cell_size <- 10

  # hard-coding this, but it doesn't matter if it is wrong because main point is that it is a static grid
  # and we'll error if any points are outside of it
  NLDAS_box <- st_sfc(st_polygon(x = list(matrix(c(0,464,0, 0,224,0), ncol = 2))))
  NLDAS_task_grid <- sf::st_make_grid(NLDAS_box, square = TRUE, cellsize = task_cell_size, offset = c(-0.5,-0.5))
  file_info <- readRDS(cell_group_file) %>%
    mutate(x = stringr::str_extract(filepath, '(?<=x\\[).+?(?=\\])'),
           y = stringr::str_extract(filepath, '(?<=y\\[).+?(?=\\])'))

  cells_sf <- sf::st_as_sf(file_info, coords = c("x", "y"))
  overlapping_boxes <- st_intersects(NLDAS_task_grid, cells_sf, sparse = F) %>% rowSums() %>% as.logical() %>% which()

  # the update we need is to create tasks that are based on the NLDAS task boxes
  # and then filter the output files and input cell files relevant to those.

  # each task is a group of driver outputs as a hash table, those all get run
  # through bind at the end as a combiner and the final result is a huge
  # hash table .ind file


  # --- task step format ---
  #box_260_driver_filepaths:
  #  command: filter_task_files(cell_group_table_file = '7_drivers_munge/out/cell_group_table.rds',
  #                             meteo_dir = I('7_drivers_munge/out'),
  #                             box_bounds = I(c(239.5, 249.5, 49.5, 59.5)))

  file_filter_step <- create_task_step(

    step_name = 'filter_files',
    target = function(task_name, step_name, ...) {

      sprintf("box_%s_driver_filepaths", task_name)
    },
    command = function(target_name, task_name, ...) {

      box_indx <- as.numeric(task_name)
      this_bbox <- st_bbox(NLDAS_task_grid[box_indx])
      sprintf('filter_task_files(cell_group_table_file = \'%s\',
      meteo_dir = I(\'%s\'),
      box_bounds = I(c(%s, %s, %s, %s)))',
              cell_group_file,
              ind_dir,
              this_bbox[['xmin']], this_bbox[['xmax']], this_bbox[['ymin']], this_bbox[['ymax']])
    }
  )

  # --- task step format ---
  #box_260_drivers_out:
  #  command: feathers_to_driver_files(driver_table = box_260_driver_filepaths)
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

create_driver_task_makefile <- function(makefile, task_plan, final_target, ...){
  include <- "7_drivers_munge.yml"
  packages <- c('dplyr', 'feather', 'readr','lubridate', 'sf')
  sources <- c(...)

  create_task_makefile(
    task_plan, makefile = makefile,
    include = include, sources = sources,
    finalize_funs = "combine_hash_tables",
    final_targets = final_target,
    file_extensions=c('ind'), packages = packages)

}
