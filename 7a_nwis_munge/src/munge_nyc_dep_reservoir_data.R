
munge_nyc_dep_temperature <- function(in_ind, out_ind, xwalk) {

  dat <- readxl::read_xlsx(in_ind) %>%
    # Removing unnecessary columns.
    dplyr::select(-c(`Surface Elevation (ft.)`, `Thermocline (m)`,
                     Analyte, Units, Qualifier)) %>%
    filter(!is.na(Value)) %>% # Filtering the NA temperature values.
    mutate(date =  as.Date(`Sample Date`), # Creating date column.
           site_id = as.character(xwalk[Reservoir])) %>%
    rename(source_id = Site, profile_id = `Profile Id`, dateTime = `Sample Date`,
           depth = `Depth (m)`, temp = Value)
  browser()
  # Filtering the sites near the dam (start with "1")
  dam_dat <- filter(dat, source_id %in% c('1WDC', '1EDP')) %>%
    mutate(hour = lubridate::hour(dateTime),
           difference_from_noon = abs(hour-12)) %>%
    # these sites are sometimes measured >1/day
    group_by(site_id, source_id, date) %>%
    filter(profile_id == profile_id[which.min(difference_from_noon)])

  # did we reduce down to a single profile per reservoir-date?
  summary(group_by(dam_dat, site_id, date) %>% summarize(n_prof = length(unique(profile_id))))

  # Selecting the unique dates associated with the sites near dam.
  cannonsville_dates <- unique(dam_dat$date[dam_dat$Reservoir == 'Cannonsville'])
  pepacton_dates <- unique(dam_dat$date[dam_dat$Reservoir == 'Pepacton'])

  # Filtering out the dam sites and dates associated with them.
  alt_sites <- dat %>%
    filter(!(Reservoir %in% 'Cannonsville' & date %in% cannonsville_dates) &
             !(Reservoir %in% 'Pepacton' & date %in% pepacton_dates)) %>%
    # Grouping by reservoir and site to get the dates of measurement per reservoir/ site.
    group_by(site_id, source_id) %>%
    mutate(n_dates = length(unique(date))) %>%
    group_by(site_id, date) %>%
    filter(n_dates == max(n_dates)) %>%
    # handle duplicated n_dates values by reducing again to the first source_id
    filter(source_id == unique(source_id)[1])

  # Reducing to one profile per day per reservoir
  # by selecting profile closest to noon
  daily_alt_dat <- alt_sites %>%
    # Selecting the profile closest to noon.
    mutate(hour = lubridate::hour(dateTime),
           difference_from_noon = abs(hour - 12)) %>%
    group_by(site_id, date) %>%
    filter(profile_id == profile_id[which.min(difference_from_noon)])

  # bind data and select rows
  dat_out <- bind_rows(dam_dat, daily_alt_dat)

  # is there a single profile per site_id, date?
  summary(group_by(dat_out, site_id, date) %>% summarize(n_prof = length(unique(profile_id))))

  # did we maintain the same number of unique dates per reservoir from the original data?
  length(unique(dat$date[dat$Reservoir == 'Cannonsville'])) == length(unique(dat_out$date[dat_out$Reservoir == 'Cannonsville']))
  length(unique(dat$date[dat$Reservoir == 'Pepacton'])) == length(unique(dat_out$date[dat_out$Reservoir == 'Pepacton']))

  # The munged dataframe has site_id, source_id, date, depth, temp columns.
  dat_out <- dat_out %>%
    # Selecting columns to order and rename them.
    dplyr::select(site_id, source_id, date, dateTime, depth, temp)

  saveRDS(dat_out, as_data_file(out_ind))
  gd_put(out_ind)
}


# Function to combine the drb_reservoirs_temp and nyc_dep_temp data.
combine_reservoirs_temperature <- function(drb_ind, nyc_dep_ind, out_ind) {

  drb_reservoirs_temps <- readRDS(sc_retrieve(drb_ind))

  nyc_det_reservoirs_temps <- readRDS(sc_retrieve(nyc_dep_ind))

  combine_dat <- bind_rows(drb_reservoirs_temps, nyc_det_reservoirs_temps)

  n_occur <- data.frame(table(combine_dat$date))
  n_occur[n_occur$Freq > 1,]
  combine_dat[combine_dat$date %in% n_occur$Var1[n_occur$Freq > 1],]

  saveRDS(combine_dat, as_data_file(out_ind))
  gd_put(out_ind)
}
