# function to retrieve list of files from google drive

files_in_drive <- function(out_ind, gd_path, dummy_date) {

  updater <- dummy_date
  gd_files <- drive_ls(path = gd_path)

  saveRDS(gd_files, file = as_data_file(out_ind))
  gd_put(out_ind)

}
