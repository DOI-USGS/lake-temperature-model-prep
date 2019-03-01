

fetch_LAGOS_NE_depths <- function(out_ind){

  data_file <- scipiper::as_data_file(out_ind)
  download.file(url = "https://portal.lternet.edu/nis/dataviewer?packageid=knb-lter-ntl.320.1&entityid=4a283c25f3548c0f78d8a01658e4a353",
                destfile = data_file)

  gd_put(out_ind, data_file)
}

fetch_LAGOS_NE_secchi <- function(out_ind){

  data_file <- scipiper::as_data_file(out_ind)
  download.file(url = "https://portal.edirepository.org/nis/dataviewer?packageid=edi.101.2&entityid=f02dd4d3375bed6871a33c648ef1631e",
                destfile = data_file)

  gd_put(out_ind, data_file)
}
