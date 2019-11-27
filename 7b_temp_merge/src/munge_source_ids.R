# cleans up source names for data release
# source IDs have a matching source metadata file with data release
# munge_source_ids was initial write of source_metadata, not part of formal pipeline
# leaving here just in case

munge_source_ids <- function() {
  sources <- all_dailies %>%
    group_by(source) %>%
    summarize(n_obs = n()) %>%
    mutate(source_id = case_when(
      grepl('historicalfiles', source) ~ 'MN_DNR_historical_files',
      grepl('Tenmile', source) ~ 'Tenmile_Lake_Bruce_Carlson',
      grepl('Carlos_DO|Greenwood_DO|South_Center_DO', source) ~ 'MN_sentinel_lakes_application',
      grepl('Cass_lake_emperature|ML_observed_temperatures|URL_Temp_Logger|Winnie', source) ~ 'MN_DNR_temperature_loggers',
      grepl('DNRdatarequest', source) ~ 'MN_DNR_fisheries_data',
      grepl('lower_red_', source) ~ 'RLBC_Lower_Red_Lake',
      grepl('Temp_Logger_Data_2015|upper_red', source) ~ 'RLBC_Upper_Red_Lake',
      grepl('Lake_Vermillion|Verm_annual|Vermilion_Logger|vermillion_repeated_tempDO|Logger_Temps_|Joes_Dock|Lake_Vermilion|Open_Water_Logger_2013', source) ~ 'MN_DNR_Lake_Vermillion',
      TRUE ~ gsub('(7a_temp_coop_munge/tmp/)(.*)(.rds)', '\\2', source, perl = TRUE)
    )) %>%
    group_by(source_id) %>%
    mutate(n_obs = sum(n_obs)) %>%
    ungroup()

  metadata <- read.csv('C:/Users/soliver/Downloads/sources.csv', stringsAsFactors = FALSE) %>%
    rename(source_id = source)

  source_metadata <- left_join(sources, metadata) %>%
    mutate(file_name = gsub('(7a_temp_coop_munge/tmp/)(.*)(.rds)', '\\2', source, perl = TRUE)) %>%
    dplyr::select(file_name, source_id, description = narrative, collection_organization = `Collection.Organization`, public_release = `publically.available`)

  write.csv(file = 'source_metadata.csv', x = source_metadata, row.names = FALSE)
}

gd_download_and_indicate <- function(source_metadata_loc, out_ind) {
  # download metadata
  drive_download(file = as_id(source_metadata_loc), path = as_data_file(out_ind), overwrite = TRUE)
  sc_indicate(ind_file = out_ind, data_file = as_data_file(out_ind))
}

add_source_ids <- function(datin_ind, metadata_ind, datout_ind) {
  metadata <- sc_retrieve(metadata_ind)
  metadata <- read.csv(metadata, stringsAsFactors = FALSE)

  dat <- feather::read_feather(sc_retrieve(datin_ind)) %>%
    mutate(file_name = gsub('(7a_temp_coop_munge/tmp/)(.*)(.rds)', '\\2', source, perl = TRUE))

  dat_source <- left_join(dat, metadata, by = 'file_name') %>%
    dplyr::select(site_id, date, depth, temp, source_id)

  feather::write_feather(dat_source, as_data_file(datout_ind))
  gd_put(datout_ind, as_data_file(datout_ind))
}

summarize_sources <- function(metadata_ind, datin_ind, datout_ind){
  metadata <- sc_retrieve(metadata_ind)
  metadata <- read.csv(metadata, stringsAsFactors = FALSE) %>%
    distinct(source_id, description, collection_organization, public_release)

  source_summary <- feather::read_feather(sc_retrieve(datin_ind)) %>%
    group_by(source_id) %>%
    summarize(n_obs = n())

  source_summary_out <-left_join(source_summary, metadata)

  write.csv(source_summary, as_data_file(datout_ind), row.names = FALSE)
  gd_put(datout_ind, as_data_file(datout_ind))
}

