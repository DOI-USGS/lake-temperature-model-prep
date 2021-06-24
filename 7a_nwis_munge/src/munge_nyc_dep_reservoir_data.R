
munge_nyc_dep_temperature <- function(out_ind, in_ind, xwalk) {

  # filter criteria
  max.temp <- 40 # threshold!
  min.temp <- 0

  dat <- readxl::read_xlsx(sc_retrieve(in_ind)) %>%
    # Removing unnecessary columns.
    dplyr::select(-c(`Surface Elevation (ft.)`, `Thermocline (m)`,
                     Analyte, Units, Qualifier)) %>%
    filter(!is.na(Value)) %>% # Filtering the NA temperature values.
    mutate(date =  as.Date(`Sample Date`), # Creating date column.
           site_id = as.character(xwalk[Reservoir])) %>%
    rename(source_id = Site, profile_id = `Profile Id`, dateTime = `Sample Date`,
           depth = `Depth (m)`, temp = Value) %>%
    distinct() %>%
    filter(temp >= min.temp,
           temp <= max.temp)

  # Filtering the sites near the dam (start with "1")
  dam_dat <- filter(dat, source_id %in% c('1WDC', '1EDP')) %>%
    mutate(time = format(dateTime, '%H:%M'),
           time_hm = lubridate::hm(time),
           hours_minus_noon = time_hm$hour - 12,
           minutes_decimal = time_hm$minute/60,
           hours_diff_noon = abs(hours_minus_noon + minutes_decimal)) %>%
    # these sites are sometimes measured >1/day
    group_by(site_id, source_id, date) %>%
    filter(profile_id == profile_id[which.min(hours_diff_noon)])

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
    mutate(time = format(dateTime, '%H:%M'),
           time_hm = lubridate::hm(time),
           hours_minus_noon = time_hm$hour - 12,
           minutes_decimal = time_hm$minute/60,
           hours_diff_noon = abs(hours_minus_noon + minutes_decimal)) %>%
    group_by(site_id, date) %>%
    filter(profile_id == profile_id[which.min(hours_diff_noon)])

  # bind data and select rows
  dat_out <- bind_rows(dam_dat, daily_alt_dat)

  # The munged dataframe has site_id, source_id, date, depth, temp columns.
  dat_out <- dat_out %>%
    # Selecting columns to order and rename them.
    dplyr::select(site_id, source_id, date, dateTime, depth, temp) %>%
    filter(depth > 0) %>%
    ungroup() %>%
    # leftover duplicates per res-date-depth have same profiles IDs
    # take average temp
    group_by(site_id, source_id, date, depth) %>%
    summarize(temp = mean(temp)) %>% ungroup()


  saveRDS(dat_out, as_data_file(out_ind))
  gd_put(out_ind)
}


munge_nyc_dep_waterlevel <- function(out_ind, temp_ind, xwalk) {

  temp_dat <- readxl::read_xlsx(sc_retrieve(temp_ind)) %>%
    dplyr::select(Reservoir, Site, `Sample Date`, `Surface Elevation (ft.)`) %>%
    filter(!is.na(`Surface Elevation (ft.)`)) %>%
    rename(surface_elevation_ft = `Surface Elevation (ft.)`) %>%
    mutate(site_id = as.character(xwalk[Reservoir]),
           date = as.Date(`Sample Date`))

  # get daily water level from temp data
  daily_levels <- temp_dat %>%
    dplyr::select(site_id, Site, date, surface_elevation_ft) %>%
    distinct() %>%
    group_by(site_id, date) %>%
    summarize(surface_elevation_m = mean(surface_elevation_ft)*0.3048,
              source_id = paste(unique(Site), collapse = '; ')) %>%
    dplyr::select(site_id, source_id, date, surface_elevation_m) %>%
    ungroup()


  saveRDS(daily_levels, as_data_file(out_ind))
  gd_put(out_ind)

}
