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

    }  else if (grepl('DNR data request_Secchi,', coop_wants[i])){
      parsers[i] <- 'parse_mndnr_files'

    }  else {
      parsers[i] <- paste0('parse_', tools::file_path_sans_ext(coop_wants[i]))
    }

  parser_exists <- parsers %in% parser_fxns

  }

  if (!all(parser_exists)) {
    stop(paste0('Cooperator data file(s) ', paste0(coop_wants[which(parser_exists == FALSE)], collapse = ', '), 'do not have parsers. Please write a parser and save to 7a_temp_coop_munge/src/data_parser'))
  }

  return(parsers)
}

create_coop_munge_taskplan <- function(wants) {

  # this step finds the appropriate parser, reads in data, parses data, then writes a .rds file and .rds.ind file
  coop_munge_step1 <- scipiper::create_task_step(
    step_name = 'parse_and_write',
    target_name = function(task_name, ...) {
      file.path('7a_temp_coop_munge/out', paste0(tools::file_path_sans_ext(task_name), '.rds.ind'))
    },
    command = paste0('parse_',
                     find_parser(target_name),
                     "(outind = '",
                     target_name,
                     "', inind = '6_temp_coop_fetch/in/",
                     as_ind_file(task_name), "'")
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
