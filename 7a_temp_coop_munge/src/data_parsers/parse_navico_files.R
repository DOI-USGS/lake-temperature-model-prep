parse_navico_files <- function(inind, outind){

  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # standardize column types - required because a subset of
  # numeric columns contain text. Text in data columns will be coerced to `NA`
  cols <- c(rep('text', 2), # state and wb name
            rep('numeric', 18), # all data columns and some metadata
            rep('date', 2)) # dates

  # read in data: identify sheets in data, read in raw data by sheet, and combine into one data set
  raw_sheets <- readxl::excel_sheets(infile)
  ls_raw <- lapply(raw_sheets, function(x){
    out <- read_xlsx(infile, sheet = x, col_types = cols)
    return(out)
  })
  raw <- bind_rows(ls_raw)

  # clean data
  clean <- raw %>%
    dplyr::filter(!is.na(`Hour (UTC)`)) %>% # remove temporally aggregated data
    dplyr::mutate(date_complete =
             lubridate::ymd_h(paste(Year, Month, Day, `Hour (UTC)`, sep = ' ')),
           DateTime = lubridate::ymd(paste(Year, Month, Day, sep = ' ')),
           TimeZone = "UTC",
           depth = 0,  # assumed surface samples
           id = paste("Navico_", MapWaterbodyId, sep = '')) %>%
    dplyr::mutate(time = format(date_complete, "%H:%M")) %>%
    dplyr::select(DateTime, time, TimeZone, depth, temp = AveWaterTempC, id)

  # save data
  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)

}
