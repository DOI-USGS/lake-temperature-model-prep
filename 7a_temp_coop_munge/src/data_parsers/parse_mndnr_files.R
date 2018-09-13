parse_mndnr_files <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)


  dat_sheets <- readxl::excel_sheets(infile)
  temp_sheets <- grep('Temp', dat_sheets, value = TRUE)

  all_dat <- data.frame()
  for (i in 1:length(temp_sheets)) {

    dat_raw <- read_excel(infile, sheet = temp_sheets[i])

    dat_cleaned <- dat_raw %>%
      dplyr::mutate(DateTime = as.Date(Date),
                    time = ifelse(is.na(Time), NA, format(Time, '%H:%M')),
                    timezone = ifelse(is.na(Time), NA, 'CST/CDT')) %>%
      dplyr::select(DateTime, time, timezone,
             depth = `Depth (m)`,
             temp = dplyr::starts_with('Temp'),
             site = Site) %>%
      dplyr::mutate(sheet = temp_sheets[i]) %>%
      dplyr::arrange(site, DateTime, depth) %>%
      dplyr::filter(!is.na(depth), !is.na(temp))

    all_dat <- dplyr::bind_rows(all_dat, dat_cleaned)
  }

  # manually found DOW numbers from here: https://maps2.dnr.state.mn.us/ewr/fom/mapper.html?layers=lakes%20streams%20wshd_lev01py3%20occurrences
  all_dat <- mutate(all_dat, DOW = case_when(
    grepl('Rainy', sheet) ~ '69069400',
    grepl('Crane', sheet) ~ '69061600',
    grepl('Kab', sheet) ~ '69084500',
    grepl('Vermilion', sheet) ~ '69060800',
    grepl('Namakan', sheet) ~ '69069300',
    grepl('Sand', sheet) ~ '69061700'
  ))

  all_dat <- select(all_dat, DateTime, time, depth, temp, DOW)

  saveRDS(object = all_dat, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)


}
