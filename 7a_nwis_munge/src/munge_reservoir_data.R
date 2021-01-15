
munge_nwis_temperature <- function(in_ind, out_ind, xwalk){

  dat <- readRDS(sc_retrieve(in_ind)) %>%
    renameNWISColumns() %>%
    dplyr::select(-c(X_.backup.from.radar._62615_Inst,
                     X_.backup.from.radar._62615_Inst_cd)) %>%
    rename(surface_elevation = X_62615_Inst, surface_elevation_cd = X_62615_Inst_cd)

  # annoying problem where surface elevation and temperature taken on same date/time,
  # but are in different rows, so NAs are populated. Resolved here.
  dat_levels <- dat %>% dplyr::select(site_no, dateTime, surface_elevation, surface_elevation_cd) %>%
    filter(!is.na(surface_elevation))

  dat_fixed <-  dplyr::select(dat, -surface_elevation, -surface_elevation_cd) %>%
    left_join(dat_levels)

  # adding the word value by temp measurement to prepare the data for pivot_longer
  names(dat_fixed)[grepl('Wtemp_Inst$', names(dat_fixed))] <-
    paste0(names(dat_fixed)[grepl('Wtemp_Inst$', names(dat_fixed))], '_value')
  # make the data long instead of wide.
  temp_dat_long <- pivot_longer(dat_fixed, cols = contains('Wtemp'),
                           names_to = c('measurement_elevation', '.value'),
                           names_pattern = '(.*Inst)_(.*)',
                           values_drop_na = TRUE) %>%
    # extract and create the elevation value that corresponds to each temp measurement.
    mutate(measurement_elevation = gsub('\\.', '', measurement_elevation),
           measurement_elevation = gsub('(.*at)(\\d+)(ft.*)', '\\2',
                                        measurement_elevation, perl = TRUE))
 # converting the measurement_elevation column to a numeric column.
  temp_dat_long$measurement_elevation = as.numeric(temp_dat_long$measurement_elevation)
  #left joining the reservoir temp data with the reservoir elevation data.

  dat_out <- temp_dat_long %>%
    mutate(depth = 0.3048*(surface_elevation - measurement_elevation), # convert to depth in meters
           time = format(dateTime, '%H:%M'),
           timezone = tz_cd,
           date = as.Date(dateTime),
           site_id = as.character(xwalk[site_no])) %>% # add nhdhr
    group_by(site_no, date) %>%
    # if any calculated depths from above are missing (because missing surface elevation)
    # use the mean surface elevation from that day
    mutate(mean_surface_elevation = mean(surface_elevation, na.rm = TRUE)) %>%
    ungroup() %>%
    mutate(depth = ifelse(is.na(depth), 0.3048*(mean_surface_elevation - measurement_elevation), depth)) %>%
    group_by(site_no, date, measurement_elevation) %>%
    # because reservoir going up and down during the day, slightly different depths each time
    # need to be able to group by depth later on to pick a single timeseries value
    # but still want to preserve actual depth
    mutate(depth_category = mean(depth, na.rm = TRUE)) %>% # create a single depth per sensor per day
    ungroup() %>%
    dplyr::select(date, time, timezone, depth, depth_category, temp = value, site_id, source_id = site_no)

  saveRDS(dat_out, as_data_file(out_ind))
  gd_put(out_ind)
}

