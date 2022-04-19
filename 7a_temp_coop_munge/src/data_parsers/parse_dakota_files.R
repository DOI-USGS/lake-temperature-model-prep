parse_SD_Lake_temp_export <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw <- readxl::read_xlsx(infile, sheet = 1)
  # no time data
  clean <- raw %>%
    filter(grepl('Temp', Parameter)) %>%
    mutate(depth = DepthToActivity_m,
           temp = ResultValue,
           DateTime = as.Date(SampleDateOnly),
           SD_ID = AU_ID) %>%
    dplyr::select(DateTime, depth, temp, SD_ID)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
