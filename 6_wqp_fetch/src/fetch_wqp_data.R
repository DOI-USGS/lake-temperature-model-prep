fetch_wqp_data <- function(out_ind, characteristicName, site_ind, dummy, ..., max_results = 500000, max_sites = 499) {

  data_tmp_dir <- dirname(out_ind) %>% str_replace('out', 'tmp')
  target_name <- scipiper::as_data_file(out_ind)
  pull_type <- basename(target_name) %>% tools::file_path_sans_ext()
  remakefile <- paste0(pull_type, '_tasks.yml')
  sources <- c(...)

  wqp_sites <- readRDS(scipiper::sc_retrieve(site_ind))

  bad_char <- filter(wqp_sites, str_detect(MonitoringLocationIdentifier, '/'))
  bad_RCE <- filter(wqp_sites, str_detect(MonitoringLocationIdentifier, 'RCE WRP-'))
  bad_ALABAMACOUSHATTA <- filter(wqp_sites, str_detect(MonitoringLocationIdentifier, 'ALABAMACOUSHATTATRIBE.TX_WQX'))
  message(sprintf("**dropping %s sites and %s results due to containing '/'", nrow(bad_char), sum(bad_char$resultCount)))
  message(sprintf("**dropping %s sites and %s results due to containing 'RCE WRP-'", nrow(bad_RCE), sum(bad_RCE$resultCount)))
  message(sprintf("**dropping %s sites and %s results due to containing 'ALABAMACOUSHATTATRIBE.TX_WQX'", nrow(bad_ALABAMACOUSHATTA), sum(bad_ALABAMACOUSHATTA$resultCount)))

  bad_sites <- c(
    bad_char$MonitoringLocationIdentifier,
    bad_RCE$MonitoringLocationIdentifier,
    bad_ALABAMACOUSHATTA$MonitoringLocationIdentifier)
  #"site_id", "MonitoringLocationIdentifier", "OrganizationIdentifier", "resultCount"
  #
  # now filter out bad sites
  wqp_good_sites <- filter(wqp_sites, !MonitoringLocationIdentifier %in% bad_sites) %>%
    arrange(desc(resultCount)) %>%
    mutate(task_num = MESS::cumsumbinning(x = resultCount, threshold = max_results, maxgroupsize = max_sites),
           task_name = paste0("pull_", task_num),
           pull_org = FALSE)

  # now create partitions for the organizations for the 'bad' ids, and append but make MonitoringLocationIdentifier NA
  wqp_site_table <- filter(wqp_sites, MonitoringLocationIdentifier %in% bad_sites) %>% group_by(OrganizationIdentifier) %>%
    mutate(task_num = max(wqp_good_sites$task_num)+cur_group_id(),
           task_name = paste0("pull_", task_num),
           pull_org = TRUE) %>% ungroup() %>%
    filter(!str_detect(OrganizationIdentifier, '/')) %>%
    rbind(wqp_good_sites, .) %>%
    arrange(task_num)

  wqp_site_file <- paste0(pull_type, '_site_table.rds') %>% file.path(data_tmp_dir, .)
  saveRDS(wqp_site_table, file = wqp_site_file)
  tasks <- wqp_site_table %>% pull(task_name) %>% unique()

  sites_step <- create_task_step(
    step_name = 'separate',
    target_name = function(task_name, step_name, ...) {
      sprintf('%s_sites', task_name)
    },
    command = function(task_name, ...){

      sprintf("filter_wqp_sites(wqp_site_file = '%s',
      task_name = I('%s'))", wqp_site_file, task_name)
    }
  )
  download_step <- create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      sprintf('%s/%s_%s.rds', data_tmp_dir, pull_type, task_name)
    },
    command = function(task_name, ...){
      sprintf("pull_wqp_data(target_name, wqp_sites = %s_sites,
      characteristicName = I('%s'), dummy = I('%s'))", task_name, paste(characteristicName, collapse = "|"), dummy)
    }
  )

  task_plan <- create_task_plan(
    task_names = tasks,
    task_steps = list(sites_step, download_step),
    final_steps = c('download'),
    add_complete = FALSE)


  # Create the task remakefile
  create_task_makefile(
    task_plan = task_plan,
    makefile = remakefile,
    packages = c('dataRetrieval', 'dplyr', 'httr'),
    sources = sources,
    final_targets = target_name,
    finalize_funs = 'bind_write_rds',
    tickquote_combinee_objects=TRUE,
    as_promises=FALSE)

  loop_tasks(task_plan = task_plan, task_makefile = remakefile, n_cores = 1)

  gd_put(out_ind, target_name)
  file.remove(remakefile, wqp_site_file)
}

filter_wqp_sites <- function(wqp_site_file, task_name){
  readRDS(wqp_site_file) %>% filter(task_name == !!task_name)
}


# hack around w/ https://github.com/USGS-R/dataRetrieval/issues/434
wqp_POST <- function(wqp_args_list){
  wqp_url <- "https://www.waterqualitydata.us/data/Result/search"

  post_body = jsonlite::toJSON(wqp_args_list, pretty = TRUE)

  download_location <- tempfile(pattern = ".zip")
  pull_metadata <- POST(paste0(wqp_url,"?mimeType=tsv&zip=yes"),
                        body = post_body,
                        content_type("application/json"),
                        accept("application/zip"),
                        httr::write_disk(download_location))

  headerInfo <- httr::headers(pull_metadata)
  unzip_location <- tempdir()
  unzipped_filename <- utils::unzip(download_location, exdir=unzip_location)
  unlink(download_location)

  dat_out <- suppressWarnings(
    read_delim(
      unzipped_filename,
      col_types = cols(`ActivityStartTime/Time` = col_character(),
                       `ActivityEndTime/Time` = col_character(),
                       USGSPCode = col_character(),
                       ResultCommentText=col_character(),
                       `ActivityDepthHeightMeasure/MeasureValue` = col_number(),
                       `DetectionQuantitationLimitMeasure/MeasureValue` = col_number(),
                       ResultMeasureValue = col_number(),
                       `WellDepthMeasure/MeasureValue` = col_number(),
                       `WellHoleDepthMeasure/MeasureValue` = col_number(),
                       `HUCEightDigitCode` = col_character(),
                       `ActivityEndTime/TimeZoneCode` = col_character()),
      quote = "", delim = "\t"))
  unlink(unzipped_filename)
  return(dat_out)
}

# pull a batch of WQP observations, save locally, return .tind file
pull_wqp_data <- function(data_file, wqp_sites, characteristicName, dummy){

  charnames <- str_split(characteristicName, '\\|')[[1]]

  # prepare the arguments to pass to readWQPdata
  wqp_args <- list()
  wqp_args$characteristicName <- charnames

  if (wqp_sites$pull_org[1]){
    wqp_args$organization <- pull(wqp_sites, OrganizationIdentifier)
  } else {
    wqp_args$siteid <- pull(wqp_sites, MonitoringLocationIdentifier)
  }


  # do the data pull
  # first pull using readWQPdata, then if that fails, try POST
  wqp_dat <- suppressMessages(wqp_POST(wqp_args))

  if (wqp_sites$pull_org[1]){
    wqp_dat <- filter(wqp_dat, MonitoringLocationIdentifier %in% wqp_sites$MonitoringLocationIdentifier)
  }
  saveRDS(wqp_dat, data_file)
}


bind_write_rds <- function(outfile, ...){
  purrr::map(c(...), function(x){
    readRDS(x)
  }) %>% purrr::reduce(rbind) %>%
    saveRDS(file = outfile)
}
