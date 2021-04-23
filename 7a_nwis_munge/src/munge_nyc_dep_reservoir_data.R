
munge_nyc_dep_temperature <- function(in_ind, out_ind, xwalk) {


  dat <- readxl::read_xlsx(in_ind) %>%
    # Removing unnecessary columns.
    dplyr::select(-c(`Thermocline (m)`, Analyte, Units, Qualifier)) %>%
    filter(!is.na(Value)) %>% # Filtering the NA temperature values.
    mutate(date =  as.Date(`Sample Date`), # Creating date column.
           site_id = as.character(xwalk[Reservoir]),
           hour = lubridate::hour(dateTime))

  sites_near_dam <- dat %>%
    filter(source_id %in% c('1WDC','1EDP')) %>%
    group_by(site_id, date, source_id) %>%
    summarize(n_profile = length(unique(profile_id)))

  dam_sites_dates <- sites_near_dam$date

  sites_non_dam<- dat %>%
    filter(!date %in% dam_sites_dates) %>%
    group_by(site_id, date, source_id) %>%
    summarize(n_profile = length(unique(profile_id)))


  dat_out <- dat %>%
    # Selecting columns to order and rename them.
    dplyr::select(date, dateTime = `Sample Date`, depth = `Depth (m)`, temp = Value,
                  site_id, source_id = Site, profile_id = `Profile Id`,
                  surface_elevation = `Surface Elevation (ft.)`, approved_On = `Approved On`)
  saveRDS(dat_out, as_data_file(out_ind))
  gd_put(out_ind)
}
