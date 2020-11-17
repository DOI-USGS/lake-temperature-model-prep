fetch_wqp_sites <- function(out_ind, characteristicName, sf_ind, box_res, dummy, ...) {

  target_name <- scipiper::as_data_file(out_ind)
  remakefile <- basename(target_name) %>% tools::file_path_sans_ext() %>% paste0('_tasks.yml')
  sources <- c(...)

  sf_geometry <- readRDS(scipiper::sc_retrieve(sf_ind))

  bbox_grid <- sf::st_make_grid(sf_geometry, square = TRUE, cellsize = box_res, offset = c(-180,15))
  overlapping_boxes <- st_intersects(bbox_grid, sf_geometry, sparse = F) %>% rowSums() %>% as.logical() %>% which()

  # Define task table rows
  tasks <- paste0('bbox_',overlapping_boxes)

  # Define task table columns
  download_step <- create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      sprintf('%s_data', task_name)
    },
    command = function(task_name, ...){
      this_idx <- as.numeric(str_remove(task_name, 'bbox_'))
      this_bbox <- st_bbox(bbox_grid[this_idx]) %>% paste(collapse = ',')

      sprintf("get_wqp_data(characteristicName = I('%s'),
      bBox = I('%s'), dummy = I('%s'))", paste(characteristicName, collapse = "|"), this_bbox, dummy)
    }
  )

  task_plan <- create_task_plan(
    task_names = tasks,
    task_steps = list(download_step),
    final_steps = c('download'),
    add_complete = FALSE)


  # Create the task remakefile
  create_task_makefile(
    task_plan = task_plan,
    makefile = remakefile,
    packages = c('dataRetrieval','dplyr'),
    sources = sources,
    final_targets = target_name,
    finalize_funs = 'bind_group_sites',
    tickquote_combinee_objects=TRUE,
    as_promises=FALSE)

  loop_tasks(task_plan = task_plan, task_makefile = remakefile)

  gd_put(out_ind, target_name)
  file.remove(remakefile)
}


get_wqp_data <- function(characteristicName, dummy, ...){

  # see issue https://github.com/USGS-R/dataRetrieval/issues/544
  charnames <- str_split(characteristicName, '\\|')[[1]]

  result <- tryCatch({
    whatWQPdata(characteristicName = charnames, siteType = "Lake, Reservoir, Impoundment", ...) %>%
      dplyr::select(MonitoringLocationIdentifier, OrganizationIdentifier, lat, lon, resultCount)
  }, error = function(err) {
    #check that this is the expected error
    if(err$message == "arguments imply differing number of rows: 1, 0"){
      # no results
      return(tibble(
        MonitoringLocationIdentifier = character(),
        OrganizationIdentifier = character(),
        lat = numeric(), lon = numeric(),
        resultCount = numeric()))
    } else {
      stop("bad news here, and I don't know how to handle it", err$message)
    }

  })

  return(result)

}

bind_group_sites <- function(fileout, ...){


  bind_rows(...) %>% dplyr::select(site_id = MonitoringLocationIdentifier, OrganizationIdentifier, resultCount, lat, lon) %>%
    st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
    saveRDS(fileout)
}
