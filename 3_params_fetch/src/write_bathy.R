save_bathy <- function(out_ind, bathy_gd_location){

  bathy_files_location = as_id(bathy_gd_location)
  bathy_list <- drive_ls(path = bathy_files_location)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(bathy_list, data_file)
  gd_put(out_ind, data_file)

}

fetch_url <- function(out_ind, url){
  data_file <- scipiper::as_data_file(out_ind)
  download.file(url = url, destfile = data_file)
  gd_put(out_ind, data_file)
}




