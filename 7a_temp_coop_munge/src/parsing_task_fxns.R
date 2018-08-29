find_parser <- function(coop_wants, parser_files) {

  parser_env <- new.env()
  sapply(parser_files, source,  parser_env)

  parser_fxns <- ls(envir = parser_env)

  parsers <- c()

  for (i in 1:length(coop_wants)) {

    if (grepl('manualentry', coop_wants[i])) {
      parsers[i] <- 'parse_manualentry_files'

    } else if (grepl('winnie', coop_wants[i], ignore.case = TRUE)) {
      parsers[i] <- 'parse_winnie_files'

    }  else if (grepl('DNRdatarequest', coop_wants[i])){
      parsers[i] <- 'parse_mndnr_files'

    }  else {
      parsers[i] <- paste0('parse_', tools::file_path_sans_ext(coop_wants[i]))
    }

  parser_exists <- parsers %in% parser_fxns

  }

  if (!all(parser_exists)) {
    stop(paste0('Cooperator data file(s) ', paste0(coop_wants[which(parser_exists == FALSE)], collapse = ', '), ' do not have parsers. Please write a parser and save to 7a_temp_coop_munge/src/data_parser'))
  }

  parsers_list <- as.list(parsers)
  names(parsers_list) <- coop_wants
  return(parsers_list)
}

create_coop_munge_taskplan <- function(wants, parsers) {

  coop_munge_step0 <- scipiper::create_task_step(
    step_name = 'identify_parser',
    target_name = function(task_name, ...) {
      sprintf('parser_%s', tools::file_path_sans_ext(task_name)) # or write to file
    },
    command = function(task_name, ...) {
      sprintf("coop_parsers[['%s']]", task_name) # if parsers list is named by tasks
    }
  )
  # this step finds the appropriate parser, reads in data, parses data, then writes a .rds file and .rds.ind file
  coop_munge_step1 <- scipiper::create_task_step(
    step_name = 'parse_and_write',
    target_name = function(task_name, ...) {
      file.path('7a_temp_coop_munge/out', paste0(tools::file_path_sans_ext(task_name), '.rds.ind'))
    },
    command = function(task_name, steps, ...) {
      sprintf("do.call(%s, outind = target_name, inind = '6_temp_coop_fetch/in/%s')", steps$identify_parser$target_name, as_ind_file(task_name))
    }
  )

  # This should first check for the out/.ind file for the parsed data? or depend on the .ind file?

  task_plan <- scipiper::create_task_plan(
    task_names = wants,
    task_steps = list(coop_munge_step0, coop_munge_step1),
    add_complete = FALSE,
    final_steps = 'parse_and_write'
  )

  return(task_plan)
}

create_coop_munge_makefile <- function(target_name, taskplan) {
  create_task_makefile(
    makefile = target_name,
    task_plan = taskplan,
    packages = c('scipiper', 'dplyr', 'readxl'),
    file_extensions = c("ind"),
    sources = c('7a_temp_coop_munge/src/data_parsers/parse_wilter_files.R',
                '7a_temp_coop_munge/src/data_parsers/parse_vermillion_files.R',
                '7a_temp_coop_munge/src/data_parsers/parse_mndow_coop_files.R',
                '7a_temp_coop_munge/src/data_parsers/parse_manualentry_files.R',
                '7a_temp_coop_munge/src/data_parsers/parse_micorps_files.R',
                '7a_temp_coop_munge/src/data_parsers/parse_mndnr_files.R'),
    ind_dir = '7a_temp_coop_munge/log',
    ind_complete = TRUE,
    include = '7a_temp_coop_munge.yml'
  )
}
