filter_coop_all <- function(file_list) {

  # this is just a filter to exclude files in gd that
  # should eventually be moved/cleaned up
  # for now, just keep all .xlsx, .txt, .csv, .accdb
  filenames <- file_list$name

  keep_files <- grep('\\.xlsx|\\.txt|\\.csv|\\.accdb', filenames, value = TRUE)

  # remove "explainer" files
  keep_files <- keep_files[-grep('explainer', keep_files, ignore.case = T)]

  return(keep_files)
}
