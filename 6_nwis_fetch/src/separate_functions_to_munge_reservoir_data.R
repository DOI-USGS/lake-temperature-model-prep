library(dataRetrieval)
library(dplyr)
library(tidyr)
library(tidyverse)

#### reading the reservoirs elevation data.
#### extracting the date from dateTime column.
#### removing some unnecessary columns.
#### find the average elevation associated with each provided date.
munge_reservoir_elevation <- function(data_in) {
  #, out_ind) {
  mod_data <- data_in %>%
    mutate(date = lubridate::date(dateTime)) %>%
    select(-c(X_.backup.from.radar._62615_00000,
              X_.backup.from.radar._62615_00000_cd)) %>%
    group_by(date) %>%
    mutate(daily_average_elevation = mean(X_62615_00000)) %>%
    rename(surface_elevation = X_62615_00000,
           surface_elevation_cd = X_62615_00000_cd) %>%
    ungroup()

  ## return reservoir temperature process data
  #saveRDS(mod_data, as_data_file(out_ind))
  #gd_put(out_ind)
  return(mod_data)
}

munge_reservoir_temp <- function(data_in) {
  #, out_ind) {
  data_in <- data_in %>%
    renameNWISColumns()
  #browser()
  # adding the word value by temp measurement to prepare the data for pivot_longer
  names(data_in)[grepl('Inst$', names(data_in))] <-
    paste0(names(data_in)[grepl('Inst$', names(data_in))], '_value')
  dat_long <- pivot_longer(data_in, cols = contains('Wtemp'),
                           names_to = c('measurement_elevation', '.value'),
                           names_pattern = '(.*Inst)_(.*)',
                           values_drop_na = TRUE) %>%
    mutate(measurement_elevation = gsub('\\.', '', measurement_elevation),
           measurement_elevation = gsub('(.*at)(\\d+)(ft.*)', '\\2',
                                        measurement_elevation, perl = TRUE))

  ## return reservoir temperature process data
  #saveRDS(dat_long, as_data_file(out_ind))
  #gd_put(out_ind)
  return(dat_long)
}


combine_data <- function(elev_data, temp_data) {
  #, out_ind) {

  temp_data$measurement_elevation = as.numeric(temp_data$measurement_elevation)
  # left joining the reservoir temp data with the reservoir elevation data.
  full_data <- left_join(temp_data, elev_data) %>%
    select(-c(date)) %>%
    # finding the measurement depth by subtracting the daily average elevation
    # form the temperature measurement elevation.
    mutate(measurement_depth = daily_average_elevation - measurement_elevation)

  ## return reservoir temperature and elevation combine data
  #saveRDS(full_data, as_data_file(out_ind))
  #gd_put(out_ind)
  return(full_data)
}
