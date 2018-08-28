parse_mndnr_files <- function(inind, outind) {

  infile <- as_data_file(inind)
  outfile <- as_data_file(outind)


  dat_sheets <- readxl::excel_sheets(infile)
  temp_sheets <- grep('Temp', dat_sheets, value = TRUE)

  all_dat <- data.frame()
  for (i in 1:length(do_sheets)) {

    dat_raw <- read_excel(infile, sheet = temp_sheets[i])

    dat_cleaned <- dat_raw %>%
      mutate(DateTime = as.Date(Date)) %>%
      select(DateTime,
             depth = `Depth (m)`,
             temp = `Temp(°C)`,
             site = Site) %>%
      mutate(sheet = temp_sheets[i]) %>%
      arrange(site, DateTime, depth) %>%
      filter(!is.na(depth), !is.na(temp))

    all_dat <- bind_rows(all_dat, dat_cleaned)
  }

  # manually found DOW numbers from here: https://maps2.dnr.state.mn.us/ewr/fom/mapper.html?layers=lakes%20streams%20wshd_lev01py3%20occurrences
  all_dat <- mutate(all_dat, dow = case_when(
    grepl('Rainy', sheet) ~ '69069400',
    grepl('Crane', sheet) ~ '69061600',
    grepl('Kab', sheet) ~ '69084500',
    grepl('Vermilion', sheet) ~ '69060800',
    grepl('Namakan', sheet) ~ '69069300',
    grepl('Sand', sheet) ~ '69061700'
  ))

  all_dat <- select(DateTime, depth, temp, dow)

  saveRDS(object = all_dat, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)


}
