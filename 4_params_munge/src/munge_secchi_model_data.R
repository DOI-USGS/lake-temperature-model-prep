munge_daily_secchi <- function(out_ind, kw_files_zip_ind) {

  cwd <- getwd()
  on.exit(setwd(cwd))

  outfile <- as_data_file(out_ind)

  kw_files_zip <- sc_retrieve(kw_files_zip_ind)
  zip_dir <- tempdir()
  kw_files <- unzip(zipfile = kw_files_zip, overwrite = TRUE, exdir = zip_dir)

  purrr::map(kw_files, function(x){
    read_csv(x, col_types = 'Dd') %>% mutate(site_id = str_remove(basename(x), pattern = '_kw.csv'))
  }) %>% purrr::reduce(bind_rows) %>%
    saveRDS(outfile)

  gd_put(out_ind, outfile)
}
