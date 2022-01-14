parse_Indiana_Glacial_Lakes_WQ_IN_DNR <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  raw <- readxl::read_excel(infile)
  clean <- raw %>%
    filter(grepl('temp', Parameter, ignore.case = TRUE)) %>%
    mutate(id = paste(Lake, County, sep = '_')) %>%
    rename('0' = Surface) %>%
    dplyr::select(-(Lake:County), -(Month:`Secchi (ft)`)) %>%
    tidyr::gather(key = 'depth', value = 'temp', -id, -Date) %>%
    mutate(temp = fahrenheit_to_celsius(temp),
           DateTime = as.Date(Date),
           depth = convert_ft_to_m(as.numeric(depth))) %>%
    dplyr::select(DateTime, depth, temp, id)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

parse_Indiana_CLP_lakedata_1994_2013 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  raw <- readxl::read_excel(infile)

  clean <- raw %>%
    mutate(id = paste(`Lake Name`, County, sep = '_'),
           DateTime = as.Date(`Date Sampled`)) %>%
    dplyr::select(DateTime, id, `Temp-0`:`T-35`) %>%
    tidyr::gather(key = 'depth', value = 'temp', -id, -DateTime) %>%
    mutate(depth = gsub('.+-', '', depth)) %>%
    mutate(depth = as.numeric(gsub('_', '\\.', depth))) %>%
    filter(!is.na(temp)) %>%
    dplyr::select(DateTime, depth, temp, id)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}

parse_Indiana_GlacialLakes_TempDOprofiles_5.6.13 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  raw <- readxl::read_excel(infile)

  clean <- raw %>%
    mutate(id = paste(vaw_name, vaw_waterID, sep = '_'),
           DateTime = as.Date(samp_startdate)) %>%
    dplyr::select(DateTime, id, depth = flc_depth, temp = flc_watertemp) %>%
    mutate(depth = convert_ft_to_m(depth),
           temp = fahrenheit_to_celsius(temp)) %>%
    filter(!depth < 0)
    dplyr::select(DateTime, depth, temp, id)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}
