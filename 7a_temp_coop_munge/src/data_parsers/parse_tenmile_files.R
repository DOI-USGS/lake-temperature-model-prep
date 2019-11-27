library(readxl)
library(dplyr)
parse_Tenmile_2019_Temperatures <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat <- readxl::read_xlsx(infile, skip = 2)

  dates <- as.Date(c('2019-06-02', '2019-06-13', '2019-06-19'))

  temp_cols <- grep('temp', names(dat), ignore.case = TRUE)
  temp_dat <- c(dat[[temp_cols[1]]], dat[[temp_cols[2]]], dat[[temp_cols[3]]])

  dat_out <- data.frame(DateTime = c(rep(dates[1], nrow(dat)), rep(dates[2], nrow(dat)), rep(dates[3], nrow(dat))),
                        depth = rep(dat$Meters, 3),
                        temp = temp_dat) %>%
    filter(!is.na(temp)) %>%
    mutate(DOW = '11041300')

  saveRDS(object = dat_out, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}

parse_Tenmile_2018_Temperatures_b <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat <- readxl::read_xlsx(infile, skip = 1)

  dates <- as.Date(c('2018-05-25', '2018-06-07', '2018-06-23', '2018-06-29', '2018-07-10', '2018-07-21', '2018-07-28', '2018-08-02', '2018-08-24', '2018-08-31','2018-09-06', '2018-09-22', '2018-10-05'))

  temp_cols <- grep('temp', names(dat), ignore.case = TRUE)
  temp_dat <- dat[temp_cols] %>%
    tidyr::gather()

  dat_out <- data.frame(DateTime = rep(dates, each = nrow(dat)),
                        depth = rep(dat$Meters, length(temp_cols)),
                        temp = temp_dat$value) %>%
    filter(!is.na(temp)) %>%
    filter(!is.na(depth)) %>%
    mutate(DOW = '11041300') %>%
    mutate(site = '202')

  saveRDS(object = dat_out, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}

parse_Tenmile_2018_PCA_October <-
  parse_Tenmile_2017_PCA_Temperatures_O2 <-
  parse_Tenmile_2017_PCA_Temperatures_Oct <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat <- readxl::read_xlsx(infile)

  dat_out <- dat %>%
    mutate(DateTime = as.Date(`Date (MM/DD/YYYY)`),
           time = format(dat$`Time (HH:MM:SS)`, '%H:%M'),
           timezone = 'CDT',
           site = '202') %>%
    dplyr::select(DateTime,
           time,
           timezone,
           depth = `Depth m`,
           temp = starts_with('Temp', vars = names(dat)),
           site) %>%
    mutate(DOW = '11041300')


  saveRDS(object = dat_out, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)


}

parse_Tenmile_2017_Temperatures_O2 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat <- readxl::read_xlsx(infile, skip = 1)

  dates <- as.Date(c('2017-05-20', '2017-05-27',
                     '2017-06-5', '2017-06-12', '2017-06-27', '2017-06-30',
                     '2017-07-08', '2017-07-28',
                     '2017-08-03', '2017-08-11', '2017-08-19', '2017-08-28',
                     '2017-09-07', '2017-09-12', '2017-09-22', '2017-09-29',
                     '2017-10-06', '2017-10-10', '2017-10-24'))

  temp_cols <- grep('temp', names(dat), ignore.case = TRUE)
  temp_dat <- dat[temp_cols] %>%
    tidyr::gather()

  dat_out <- data.frame(DateTime = rep(dates, each = nrow(dat)),
                        depth = rep(dat$`Depth - M`, length(temp_cols)),
                        temp = temp_dat$value) %>%
    filter(!is.na(temp)) %>%
    filter(!is.na(depth)) %>%
    mutate(DOW = '11041300') %>%
    mutate(site = '202')

  saveRDS(object = dat_out, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Tenmile_2016_Temperatures_O2 <-
  parse_Tenmile_2015_Temperatures_O2 <-
  parse_Tenmile_2014_Temperatures_O2 <-
  parse_Tenmile_2013_Temperatures_O2 <-
  parse_Tenmile_2011_Site202_Temp_O2 <-
  parse_Tenmile_2010_Site_202_TempO2 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat <- readxl::read_xlsx(infile, skip = 1)
  dates <- grep('\\d{4,}', names(readxl::read_xlsx(infile, n_max = 1)), value = TRUE)
  dates <- as.Date(as.numeric(dates), origin = '1904-01-01')

  temp_cols <- grep('temp|tamp', names(dat), ignore.case = TRUE)
  temp_dat <- dat[temp_cols] %>%
    tidyr::gather()

  depth_col <- dat[[grep('depth|meters', names(dat), ignore.case = TRUE)]]

  dat_out <- data.frame(DateTime = rep(dates, each = nrow(dat)),
                        depth = rep(depth_col, length(temp_cols)),
                        temp = temp_dat$value) %>%
    filter(!is.na(temp)) %>%
    filter(!is.na(depth)) %>%
    mutate(DOW = '11041300') %>%
    mutate(site = '202')

    dat_out$temp <- gsub('-', '', dat_out$temp)

  saveRDS(object = dat_out, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
  }

parse_Tenmile_2011_Temp_O2 <-
  parse_Tenmile_2010Temp_and_O2_Carlsonsite <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat <- readxl::read_xlsx(infile, skip = 1)
  dates <- grep('\\d{4,}', names(readxl::read_xlsx(infile, n_max = 1)), value = TRUE)
  dates <- as.Date(as.numeric(dates), origin = '1904-01-01')

  temp_cols <- grep('temp|tamp', names(dat), ignore.case = TRUE)
  temp_dat <- dat[temp_cols] %>%
    tidyr::gather()

  if (any(grepl('depth|meters', names(dat)))) {
    depth_col <- dat[[grep('depth|meters', names(dat), ignore.case = TRUE)]]
  } else {
      depth_col <- dat[[1]]
    }

  dat_out <- data.frame(DateTime = rep(dates, each = nrow(dat)),
                        depth = rep(depth_col, length(temp_cols)),
                        temp = temp_dat$value) %>%
    filter(!is.na(temp)) %>%
    filter(!is.na(depth)) %>%
    mutate(DOW = '11041300') %>%
    mutate(site = 'Carlson Site')

  saveRDS(object = dat_out, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Tenmile_2011Nov8_MPCA_data <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat <- readxl::read_xls(infile)

  dat_out <- dat %>%
    mutate(DateTime = as.Date('2011-11-08'),
           time = '11:50',
           timezone = 'CDT',
           site = '202') %>%
    dplyr::select(DateTime,
           time,
           timezone,
           depth = `Depth1`,
           temp = Temp,
           site) %>%
    mutate(DOW = '11041300')

  saveRDS(object = dat_out, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Tenmile_2009_Temperatures <-
  parse_Tenmile_2008_Temperatures_and_Oxygen <- function(inind, outind) {

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat1 <- readxl::read_xls(infile, skip = 2, sheet = 1)
  dat2 <-  readxl::read_xls(infile, skip = 2, sheet = 2)

  dates <- c(grep('\\d{4,}', names(readxl::read_xls(infile, skip = 1, n_max = 1, sheet = 1)), value = TRUE),
             grep('\\d{4,}', names(readxl::read_xls(infile, skip = 1, n_max = 1, sheet = 2)), value = TRUE))

  if (grepl('2009', inind)) {
    dates <- c(as.Date(as.numeric(dates), origin = '1899-12-30'), as.Date("2009-08-30"))

  } else if (grepl('2008', inind)) {
    dates <- c(as.Date(as.numeric(dates), origin = '1899-12-30'), as.Date(c("2008-09-04", "2008-09-09", "2008-09-11")))

  }
  dates <- sort(dates)

  # first sheet
  temp_cols1 <- grep('temp|tamp', names(dat1), ignore.case = TRUE)
  temp_dat1 <- dat1[temp_cols1] %>%
    tidyr::gather()

  # second sheet
  temp_cols2 <- grep('temp|tamp', names(dat2), ignore.case = TRUE)
  temp_dat2 <- dat2[temp_cols2] %>%
    tidyr::gather()

  temp_dat <- bind_rows(temp_dat1, temp_dat2)

  depth_col <- dat1[[grep('depth|meters', names(dat1), ignore.case = TRUE)]]

  dat_out <- data.frame(DateTime = rep(dates, each = nrow(dat1)),
                        depth = rep(depth_col, length(dates)),
                        temp = temp_dat$value) %>%
    filter(!is.na(temp)) %>%
    filter(!is.na(depth)) %>%
    mutate(DOW = '11041300')

  saveRDS(object = dat_out, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Tenmile_2007_Temperatures <-
  parse_Tenmile_2006_Temperatures <-
  parse_Tenmile_2005_Temperatures <-
  parse_Tenmile_2004_Temperatures <-
  parse_Tenmile_2003_Temperatures <-
  parse_Tenmile_2002_Temperatures <-
  parse_Tenmile_2001_Temperatures <-
  parse_Tenmile_2000_Temperatures <-
  parse_Tenmile_1999_Temperatures <-
  parse_Tenmile_1998_Temperatures <-
  parse_Tenmile_1997_Temperatures <-
  parse_Tenmile_1996_Temperatures <-


  function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  dat <- readxl::read_xls(infile, skip = 1)
  if (grepl('1998', inind)) {dat <- readxl::read_xls(infile, skip = 1, n_max = 12)}
  dates <- grep('\\d{4,}', names(readxl::read_xls(infile, n_max = 1, skip = 1)), value = TRUE)

  # it looks like some of the files have 2006 year, though the title of the file suggests a different year
  # followed up with Bruce Carlson and confirmed the file title has the correct year
  if (grepl('2005|2006', inind)) {
    dates <- as.Date(as.numeric(dates), origin = '1899-12-30')
  } else if (grepl('2007', inind)) {
    # somehow the origin for the dates in 2007 changes by cell?!?
    # hard coding these in
    dates <- as.Date(c('2007-05-22', '2007-05-30', '2007-06-05', '2007-06-12', '2007-06-30', '2007-07-07', '2007-07-15', '2007-07-21', '2007-07-28', '2007-08-03', '2007-08-10', '2007-08-22', '2007-08-28', '2007-08-31', '2007-09-12', '2007-09-16'))
  } else if (grepl('2004', inind)) {
    dates <- as.Date(as.numeric(dates), origin = '1897-12-30')

  } else if (grepl('2003', infile)) {

    dates <- as.Date(as.numeric(dates), origin = '1896-12-29')

  } else if (grepl('2002', inind)) {

    dates <- as.Date(as.numeric(dates), origin = '1895-12-30')

  } else if  (grepl('2001', inind)) {

    dates <- as.Date(as.numeric(dates), origin = '1894-12-30')

  } else if (grepl('2000', inind)) {

    dates <- as.Date(as.numeric(dates), origin = '1893-12-30')

  } else if (grepl('1999', inind)) {

    dates <- as.Date(as.numeric(dates), origin = '1892-12-29')

  } else if (grepl('1998', inind)) {

    dates <- as.Date(as.numeric(dates), origin = '1891-12-30')

  } else if (grepl('1997', inind)) {

    dates <- as.Date(as.numeric(dates), origin = '1890-12-30')

  } else if (grepl('1996', inind)) {

    dates <- as.Date(as.numeric(dates), origin = '1889-12-30')
  }

  dat_out <- dat %>%
    mutate(depth = feet_to_meters(Feet)) %>%
    dplyr::select(-Feet) %>%
    tidyr::gather(key = key, value = temp, -depth) %>%
    mutate(DateTime = rep(dates, each = nrow(dat)),
           temp = fahrenheit_to_celsius(temp)) %>%
    filter(!is.na(temp)) %>%
    filter(!is.na(depth)) %>%
    mutate(DOW = '11041300') %>%
    dplyr::select(-key)


  saveRDS(object = dat_out, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

