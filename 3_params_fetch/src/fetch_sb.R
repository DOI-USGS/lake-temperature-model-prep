fetch_sb <- function(out_ind, sb_ids) {
  dat_out <- data.frame()
  dest <- '3_params_fetch/in'
  for (id in sb_ids) {
    files <- sbtools::item_list_files(id)
    files <- files[grepl('.csv', files$fname), ]
    in_path <- file.path(dest, files$fname)
    sbtools::item_file_download(id, names = files$fname, destinations = in_path, overwrite_file = TRUE)

    dat <- readr::read_csv(in_path) %>%
      mutate(reservoir = gsub('_Capacity.csv', '', files$fname))

    dat_out <- bind_rows(dat_out, dat)
  }

  saveRDS(dat_out, as_data_file(out_ind))
  gd_put(out_ind)
}
