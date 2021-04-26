
munge_nyc_dep_temperature <- function(in_ind, out_ind, xwalk) {


  dat <- readxl::read_xlsx(in_ind) %>%
    # Removing unnecessary columns.
    dplyr::select(-c(`Thermocline (m)`, Analyte, Units, Qualifier)) %>%
    filter(!is.na(Value)) %>% # Filtering the NA temperature values.
    mutate(date =  as.Date(`Sample Date`), # Creating date column.
           site_id = as.character(xwalk[Reservoir]),
           hour = lubridate::hour(dateTime))

  sites_near_dam <- dat %>%
    filter(source_id %in% c('1WDC','1EDP'))

  cannonsville_dates <- filter(dat, source_id %in% '1WDC') %>%
    select(date) %>% unique()

  pepacton_dates <- filter(dat, source_id %in% '1EDP') %>%
    select(date) %>% unique()

  dates_count <- dat %>%
    filter(!source_id %in% '1WDC' & !date %in% cannonsville_dates &
             !source_id %in% '1EDP' & !date %in% pepacton_dates) %>%
    group_by(site_id, source_id) %>%
    mutate(n_dates = length(unique(date))) %>%
    ungroup()

  #max_dates_count <- max(dates_count$n_dates)

  sites_max_dates <- dates_count %>%
    group_by(site_id, date) %>%
    slice(which.max(n_dates))

  dat_out <- dat %>%
    # Selecting columns to order and rename them.
    dplyr::select(date, dateTime = `Sample Date`, depth = `Depth (m)`, temp = Value,
                  site_id, source_id = Site, profile_id = `Profile Id`,
                  surface_elevation = `Surface Elevation (ft.)`, approved_On = `Approved On`)
  saveRDS(dat_out, as_data_file(out_ind))
  gd_put(out_ind)
}
