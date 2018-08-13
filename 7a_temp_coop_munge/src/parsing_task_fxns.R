find_parser <- function(file_for_parsing) {

  if (grepl('manualentry', file_for_parsing)) {
    parser <- 'manualentry_files'
  } else {
    parser <- file_for_parsing
  }

  # list all functions in environment - not sure if this will work? e.g., does source
  # in remake file load functions into environment?
  all_functions <- lsf.str()

  # stop build if there is no parser for a certain data file. May want to change this behavior?
  if (!(parser %in% all_functions)) {
    stop(paste("Cooperator data file", file_for_parsing, "does not have an associated parser. Please write a parser and save to '7a_temp_coop_munge/src'."))
  }

  return(parser)
}

create_coop_taskplan <- function(wants) {

  # this step finds the appropriate parser, reads in data, parses data, then writes a .rds file and .rds.ind file
  coop_munge_step1 <- scipiper::create_task_step(
    step_name = 'parse_and_write',
    target_name = function(task_name, ...) {
      file.path('7a_temp_coop_munge/out', paste0(tools::file_path_sans_ext(task_name), '.rds.ind'))
    },
    command = paste0('parse_', find_parser(target_name), "(outind = '", target_name, "', inind = '6_temp_coop_fetch/in/", as_ind_file(task_name), "'")
  )

  # This should first check for the out/.ind file for the parsed data? or depend on the .ind file?

  task_plan <- scipiper::create_task_plan(
    task_names = wants,
    task_steps = coop_munge_step1,
    add_complete = FALSE
  )

  return(task_plan)
}

create_coop_fetch_makefile <- function(target_name, taskplan) {
  create_task_makefile(
    makefile = target_name,
    task_plan = taskplan,
    include = '6_temp_coop_fetch.yml',
    packages = c('scipiper'),
    file_extensions = c("ind"),
    ind_dir = '6_temp_coop_fetch/log',
    ind_complete = TRUE
  )
}
