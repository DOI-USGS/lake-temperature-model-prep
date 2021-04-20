
munge_nyc_dep_temperature <- function(in_ind, out_ind, xwalk) {


  dat <- readxl::read_xlsx(in_ind) %>%
    # Removing unnecessary columns.
    dplyr::select(-c(`Thermocline (m)`, `Profile Id`, Analyte, Units)) %>%
    filter(!is.na(Value)) # Filtering the NA temperature values.

  dat_out <- dat %>%
    mutate(date =  as.Date(`Sample Date`), # Creating date column.
           site_id = as.character(xwalk[Reservoir])) %>%
    # Selecting columns to order and rename them.
    dplyr::select(date, dateTime = `Sample Date`, depth = `Depth (m)`, temp = Value,
                  site_id = Reservoir, source_id = Site,
                  surface_elevation = `Surface Elevation (ft.)`, Qualifier,
                  approved_On = `Approved On`)
  saveRDS(dat_out, as_data_file(out_ind))
  gd_put(out_ind)
}
