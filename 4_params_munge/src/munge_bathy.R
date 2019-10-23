
munge_wbic_bathy <- function(out_ind, bathy_zip_ind, wbic_xwalk_ind){

  wbic_xwalk <- sc_retrieve(wbic_xwalk_ind) %>% readRDS()
  bth_dir <- tempdir()
  bathy_data <- sc_retrieve(bathy_zip_ind) %>% unzip(exdir = bth_dir) %>%
    purrr::map(function(x) {
      WBIC_ID <- basename(x) %>% str_extract('WBIC_[0-9]+')
      read_tsv(x, col_types = 'dd') %>% mutate(WBIC_ID = WBIC_ID)
    }) %>% purrr::reduce(rbind) %>%
    inner_join(wbic_xwalk, by = 'WBIC_ID') %>% dplyr::select(site_id, depths = depth, areas = area)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(bathy_data, data_file)
  gd_put(out_ind, data_file)
}

munge_mndow_bathy <- function(out_ind, bathy_list_ind, mndow_xwalk_ind){

  bathy_list <- readRDS(sc_retrieve(bathy_list_ind))

  mndow_xwalk <- readRDS(sc_retrieve(mndow_xwalk_ind))

  bathy_dat <- NULL

  # Loop through files and bind together
  for (i in 1:nrow(bathy_list)){
    csv_name <- bathy_list$name[i]
    csv_id <- as_id(bathy_list$id[i])

    lake_dow <- unlist(strsplit(csv_name, ".csv"))
    lake_dow <- unlist(strsplit(lake_dow, "_"))
    lake_dow <- lake_dow[length(lake_dow)]


    # Create folder to save downloaded files
    dir.create("3_params_fetch/in/hypsos_m/", showWarnings = FALSE)
    # Download csv's

    if (!file.exists(paste0("3_params_fetch/in/hypsos_m/",csv_name))){
      drive_download(csv_id, path=paste0("3_params_fetch/in/hypsos_m/",csv_name), overwrite = TRUE)
      message(paste0("Downloading ", csv_name))
    }

    #gd_confirm_posted(as_ind_file(paste0("3_params_fetch/in/hypsos_m/",csv_name)))
    #gd_get(as_ind_file(paste0("3_params_fetch/in/hypsos_m/",csv_name)))


    # Read csv's
    message(paste0("Reading ", csv_name))

    lake_hypso <- read.csv(paste0("3_params_fetch/in/hypsos_m/",csv_name), header=T)
    # Add lake DOW
    lake_hypso$MNDOW_ID <- paste0('mndow_', lake_dow)

    # Bind lakes together - there is a slicker way to do this
    bathy_dat <- rbind(bathy_dat, lake_hypso)

  }

  bathy_dat <- bathy_dat %>% left_join(mndow_xwalk, by = 'MNDOW_ID') %>%
    dplyr::select(site_id, depths, areas)


  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(bathy_dat, data_file)
  gd_put(out_ind, data_file)
}
