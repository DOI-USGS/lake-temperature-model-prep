
munge_nyc_dep_temperature <- function(in_ind, out_ind, xwalk) {


  dat <- readxl::read_xlsx(in_ind) %>%
    # Removing unnecessary columns.
    dplyr::select(-c(`Surface Elevation (ft.)`, `Thermocline (m)`, `Profile Id`,
                     Analyte, Units, Qualifier)) %>%
    filter(!is.na(Value)) %>% # Filtering the NA temperature values.
    mutate(date =  as.Date(`Sample Date`), # Creating date column.
           site_id = as.character(xwalk[Reservoir])) %>%
    rename(source_id = Site, dateTime = `Sample Date`,
           depth = `Depth (m)`, temp = Value)

  # Filtering the sites near the dam (start with "1")
  cannonsville_dam_site <- filter(dat, source_id %in% '1WDC')
  pepacton_dam_site <- filter(dat, source_id %in% '1EDP')

  # Selecting the unique dates associated with the sites near dam.
  cannonsville_dates <- unique(cannonsville_dam_site$date)
  pepacton_dates <- unique(pepacton_dam_site$date)

  # Filtering out the dam sites and dates associated with them.
  dates_count <- dat %>%
    filter(!source_id %in% '1WDC' & !date %in% cannonsville_dates &
             !source_id %in% '1EDP' & !date %in% pepacton_dates) %>%
    # Grouping by reservoir and site to get the dates of measurement per reservoir/ site.
    group_by(site_id, source_id) %>%
    mutate(n_dates = length(unique(date))) %>%
    ungroup()

  # Finding the site with the maximum number of days with observation (n_dates).
  max_dates <- dates_count %>%
    group_by(site_id, date) %>%
    filter(n_dates == max(n_dates)) %>%
    ungroup()

  # Selecting the site with maximum n_dates.
  sites_max_dates <- unique(max_dates$source_id)

  # Filtering the data to one profile per day per reservoir per site.
  daily_profile <- dat %>%
    # Filtering to the sites with the maximum n_dates
    filter(source_id %in% sites_max_dates) %>%
    # Binding the sites near dam.
    bind_rows(cannonsville_dam_site) %>%
    bind_rows(pepacton_dam_site) %>%
    # Selecting the profile closest to noon.
    mutate(hour = lubridate::hour(dateTime),
           difference_from_noon = abs(hour - 12)) %>%
    group_by(site_id, date) %>%
    filter(difference_from_noon == min(difference_from_noon))

  # The munged dataframe has site_id, source_id, date, depth, temp columns.
  dat_out <- daily_profile %>%
    # Selecting columns to order and rename them.
    dplyr::select(date, dateTime, depth, temp, site_id, source_id)

  saveRDS(dat_out, as_data_file(out_ind))
  gd_put(out_ind)
}

# Function to combine the drb_reservoirs_temp and nyc_dep_temp data.
combine_reservoirs_temperature <- function(drb_ind, nyc_dep_ind, out_ind) {

  drb_reservoirs_temps <- readRDS(sc_retrieve(drb_ind))
  nyc_det_reservoirs_temps <- readRDS(sc_retrieve(nyc_dep_ind))

  dat_out <- bind_rows(drb_reservoirs_temps, nyc_det_reservoirs_temps)
  saveRDS(dat_out, as_data_file(out_ind))
  gd_put(out_ind)
}
