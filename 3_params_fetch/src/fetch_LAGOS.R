

fetch_LAGOS_NE_depths <- function(out_ind){

  data_file <- scipiper::as_data_file(out_ind)
  download.file(url = "https://portal.lternet.edu/nis/dataviewer?packageid=knb-lter-ntl.320.1&entityid=4a283c25f3548c0f78d8a01658e4a353",
                destfile = data_file)

  gd_put(out_ind, data_file)
}
