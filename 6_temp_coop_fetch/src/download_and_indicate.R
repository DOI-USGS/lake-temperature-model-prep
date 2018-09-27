download_and_indicate <- function(in_ind, url, push_location) {
  local_file = as_data_file(in_ind)

  # get data from LTER website
  download.file(url = url, destfile = local_file)

  #indicate downloaded file
  sc_indicate(ind_file = in_ind, data_file = local_file)

  # push to GD coop 'in' folder where all temp data are uploaded
  googledrive::drive_upload(media = local_file, path = push_location)
}
