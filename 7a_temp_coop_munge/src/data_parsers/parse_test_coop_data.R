parse_test_coop_data <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  raw_file <- read.csv(infile, header=T)

    clean <- raw_file %>%
    mutate(date = as.Date(date, format = "%m/%d/%Y"), temp=fahrenheit_to_celsius(temp_deg_f), depth = feet_to_meters(as.numeric(depth_ft)), time=as.character(time), lakeid=as.character(lakeid)) %>%
    rename(DateTime = date, time = time, depth = depth, temp = temp, id=lakeid) %>%
    dplyr::select(DateTime, time, depth, temp, id)

  saveRDS(object = clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}
