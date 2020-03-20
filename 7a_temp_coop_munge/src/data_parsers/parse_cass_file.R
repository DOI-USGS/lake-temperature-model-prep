parse_Cass_DO_and_Temp_Profiles <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat_raw <- readxl::read_excel(infile)

  # Notes:
  #   No `time` available in original data
  #   Has DO column (`DO (mg/L)`), unsure if I should include it
  #   Not sure about what should go in the `site`` column, so I used the column actually called `Site`

  dat_cleaned <- dat_raw %>%
    dplyr::mutate(DateTime = as.Date(`Sample Date`),
                  # Make DOW a character string, and should be 7 long
                  MN_DOW = sprintf("%07d", MN_DOW)) %>%
    dplyr::select(DateTime,
                  depth = `Depth (m)`,
                  temp = `Temp (C)`,
                  DOW = MN_DOW,
                  site = Site) %>%
    dplyr::arrange(site, DateTime, depth) %>%
    dplyr::filter(!is.na(depth), !is.na(temp)) %>%
    dplyr::select(DateTime, temp, depth, DOW, site)

  saveRDS(object = dat_cleaned, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
