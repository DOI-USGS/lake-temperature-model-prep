
#' These functions were originally used to download the files
#' `3_params_fetch/out/LAGOS_NE_depths.csv` & `3_params_fetch/out/LAGOS_NE_secchi.csv`
#' but the portal website changed and now a `httr::GET()` call does not work.
#' The files remain in `out/` but future files from these portals will need to be
#' added to `in/` folders followed by an `scipiper::sc_indicate()` call. This
#' happened in April 2022 to add `3_params_fetch/in/LAGOS_NE_depths.csv`

fetch_LAGOS_NE_depths <- function(out_ind){

  data_file <- scipiper::as_data_file(out_ind)
  httr::GET(url = "https://portal.edirepository.org/nis/dataviewer?packageid=edi.101.3&entityid=df2f94197ed33bc6f3052511b23a721e",
            write_disk(data_file, overwrite=TRUE))

  read_csv(data_file) %>% filter(!is.na(maxdepth)) %>%
    rename(zmaxobs = maxdepth) %>% arrange(lagoslakeid) %>% write_csv(path = data_file)

  gd_put(out_ind, data_file)
}

fetch_LAGOS_NE_secchi <- function(out_ind){

  data_file <- scipiper::as_data_file(out_ind)
  httr::GET(url = "https://portal.edirepository.org/nis/dataviewer?packageid=edi.101.2&entityid=f02dd4d3375bed6871a33c648ef1631e",
            write_disk(data_file, overwrite=TRUE))
  gd_put(out_ind, data_file)
}
