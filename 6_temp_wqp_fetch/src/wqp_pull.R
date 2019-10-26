#### pull ####

# we've been asked to please NOT use a cluster because a postdoc took down the
# entire WQP a couple of years ago by doing that, and 12 simultaneous requests
# would be way too many. Alternative ways we can speed up the pull:
# * subset spatially rather than temporally - loop over state/HUC rather than years
# * probably fastest to pull all variables at once
# * probably faster to request whole states at once rather than giving explicit site lists
# we'll follow the first two guidelines but won't be able to make use of the third this time

# prepare a plan for downloading (from WQP) and posting (to GD) one data file
# per state
plan_wqp_pull <- function(partitions_ind, wqp_charnames_obj) {

  folders <- list(
    tmp='6_temp_wqp_fetch/tmp',
    out='6_temp_wqp_fetch/out',
    log='6_temp_wqp_fetch/log')
  partitions <- feather::read_feather(scipiper::sc_retrieve(partitions_ind, remake_file = '6_temp_wqp_fetch.yml'))

  # after all wanted data have been pulled, this function will be called but
  # doesn't need to create anything much, so just return NULL
  # isolate the partition info for just one task
  partition <- scipiper::create_task_step(
    step_name = 'partition',
    target_name = function(task_name, step_name, ...) {
      sprintf('partition_%s', task_name)
    },
    command = function(task_name, ...) {
      sprintf("filter_partitions(partitions_ind='%s', I('%s'))", partitions_ind, task_name)
    }
  )

  # download from WQP, save, and create .ind file promising the download is
  # complete, with extension .tind ('temporary indicator') because we don't want
  # scipiper to make a build/status file for it

  download <- scipiper::create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      file.path(folders$tmp, sprintf('%s.rds', task_name))
    },
    command = function(steps, ...) {
      paste(
        "get_wqp_data(",
        "data_file=target_name,",
        sprintf("partition=%s,", steps$partition$target_name),
        sprintf("wqp_pull_params=%s)", wqp_charnames_obj),
        sep="\n      ")
    }
  )



  # put the steps together into a task plan
  task_plan <- scipiper::create_task_plan(
    task_names=sort(partitions$PullTask),
    task_steps=list(partition, download),
    final_steps=c('download'),
    add_complete=FALSE,
    ind_dir=folders$tmp)

}

create_wqp_pull_makefile <- function(makefile, task_plan, final_targets) {


  create_task_makefile(
    makefile=makefile, task_plan=task_plan,
    include = c('6_temp_wqp_fetch.yml'),
    packages=c('dplyr', 'dataRetrieval', 'feather', 'scipiper', 'yaml', 'stringr', 'jsonlite', 'httr'),
    file_extensions=c('ind','feather'), finalize_funs = 'combine_wqp_data', final_targets = final_targets)
}

# extract and post the data (dropping the site info attribute), creating an
# .ind file that we will want to share because it represents the shared cache
combine_wqp_data <- function(ind_file, ...){

  rds_files <- c(...)
  df_list <- list()

  for (i in seq_len(length(rds_files))){
    df_list[[i]] <- readRDS(rds_files[i])
  }

  wqp_df <- do.call("rbind", df_list)

  data_file <- scipiper::as_data_file(ind_file)
  saveRDS(wqp_df, data_file)
  gd_put(ind_file, data_file)
}

# --- functions used in task table ---

# read the partitions file and pull out just those sites that go with a single
# PullTask and are worth requesting because they have >0 observations for that
# ParamGroup
filter_partitions <- function(partitions_ind, pull_task) {
  partitions <- feather::read_feather(sc_retrieve(partitions_ind))

  these_partitions <- dplyr::filter(partitions, PullTask==pull_task, resultCount > 0)
  bad_monIDs <- these_partitions %>% filter(stringr::str_extract(MonitoringLocationIdentifier, "/") == "/")
  if (nrow(bad_monIDs) > 0){
    message(sprintf("**dropping %s sites and %s results due to bad MonIDs", nrow(bad_monIDs), sum(bad_monIDs$resultCount)))
  }

  these_partitions %>% filter(!MonitoringLocationIdentifier %in% bad_monIDs$MonitoringLocationIdentifier)
}

# pull a batch of WQP observations, save locally, return .tind file
get_wqp_data <- function(data_file, partition, wqp_pull_params, verbose = FALSE) {

  # prepare the arguments to pass to readWQPdata
  wqp_args <- list()
  wqp_args$characteristicName <- wqp_pull_params
  wqp_args$siteid <- partition$MonitoringLocationIdentifier
  # do the data pull
  wqp_dat_time <- system.time({
    wqp_dat <- wqp_POST(wqp_args)
  })
  if (verbose){
    message(sprintf(
      'WQP pull for %s took %0.0f seconds and returned %d rows',
      partition$PullTask[1],
      wqp_dat_time['elapsed'],
      nrow(wqp_dat)))
  }
  # make wqp_dat a tibble, converting either from data.frame (the usual case) or
  # NULL (if there are no results)
  wqp_dat <- as_data_frame(wqp_dat)

  # write the data to rds file. do this even if there were 0
  # results because remake expects this function to always create the target
  # file
  saveRDS(wqp_dat, data_file)
}

# hack around w/ https://github.com/USGS-R/dataRetrieval/issues/434
wqp_POST <- function(wqp_args_list){
  wqp_url <- "https://www.waterqualitydata.us/Result/search"


  wqp_args_list$siteid <- wqp_args_list$siteid
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
  if (length(unzipped_filename) == 0){
    stop('error extracting from zip file')
  }
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
