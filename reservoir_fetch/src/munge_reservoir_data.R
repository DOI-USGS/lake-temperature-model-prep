library(dataRetrieval)
library(dplyr)
library(tidyr)
library(tidyverse)

#### reading the reservoirs elevation and temp data. .
munge_reservoir_data <- function(elev_data, temp_data){
                                 #out_ind) {
  elev_data_mod <- elev_data %>%
    #### extracting the date from dateTime column.
    mutate(date = lubridate::date(dateTime)) %>%
    #### removing unnecessary columns.
    select(-c(X_.backup.from.radar._62615_00000,
              X_.backup.from.radar._62615_00000_cd)) %>%
    group_by(date) %>%
    #### find the average daily elevation.
    mutate(daily_average_elevation = mean(X_62615_00000)) %>%
    rename(surface_elevation = X_62615_00000,
           surface_elevation_cd = X_62615_00000_cd) %>%
    ungroup()

  temp_data_mod <- temp_data %>%
    renameNWISColumns()
  # adding the word value by temp measurement to prepare the data for pivot_longer
  names(temp_data_mod)[grepl('Inst$', names(temp_data_mod))] <-
    paste0(names(temp_data_mod)[grepl('Inst$', names(temp_data_mod))], '_value')
  # make the data long instead of wide.
  temp_dat_long <- pivot_longer(temp_data_mod, cols = contains('Wtemp'),
                           names_to = c('measurement_elevation', '.value'),
                           names_pattern = '(.*Inst)_(.*)',
                           values_drop_na = TRUE) %>%
    # extact and create the elevation value that crossponds to each temp measurement.
    mutate(measurement_elevation = gsub('\\.', '', measurement_elevation),
           measurement_elevation = gsub('(.*at)(\\d+)(ft.*)', '\\2',
                                        measurement_elevation, perl = TRUE))
 # converting the measurement_elevation column to a numeric column.
  temp_dat_long$measurement_elevation = as.numeric(temp_dat_long$measurement_elevation)
  #left joining the reservoir temp data with the reservoir elevation data.

  full_reservoir_data <- left_join(temp_dat_long, elev_data_mod) %>%
    select(-c(date)) %>%
    # finding the measurement depth by subtracting the daily average elevation
    # form the temperature measurement elevation.
    mutate(measurement_depth = daily_average_elevation - measurement_elevation)

  ## return reservoir process data
  #saveRDS(full_reservoir_data, as_data_file(out_ind))
  #gd_put(out_ind)
   return(full_reservoir_data)
}

