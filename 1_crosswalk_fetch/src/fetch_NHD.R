
temp_zip_dir <- '1_crosswalk_fetch/.local_cache'

fetch_state_nhd_HR <- function(states){


  for (state in states){
    download.file(sprintf(base_url, state), destfile = tempfile(paste0(state, '.zip')))
  }

}

create_nhd_HR_download_plan <- function(states){
  base_url <- 'ftp://rockyftp.cr.usgs.gov/vdelivery/Datasets/Staged/Hydrography/NHD/State/HighResolution/Shape/NHD_H_%s_State_Shape.zip'

  download_step <- create_task_step(
    step_name = 'download',
    target_name = function(task_name, step_name, ...) {
      sprintf('%s/NHD_H_%s.zip', temp_zip_dir, task_name)
    },
    command = function(task_name, step_name, ...) {
      state_url <- sprintf(base_url, task_name)
      sprintf("download.file('%s', destfile = target_name)", state_url)
    }
  )


  create_task_plan(states, list(download_step), final_steps='download')
}

create_nhd_HR_download_makefile <- function(makefile, task_plan){
  include <- "1_crosswalk_fetch.yml"
  packages <- c('dplyr','sf')
  sources <- '1_crosswalk_fetch/src/fetch_NHD.R'

  create_task_makefile(task_plan, makefile, include = include, packages = packages, sources = sources)
}
