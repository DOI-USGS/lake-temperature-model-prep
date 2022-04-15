parse_Solomon <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)
  # confirmed all depthClass is "point" and all depthTop and depthBottom are equal

  read_csv(infile) %>% filter(!is.na(temp)) %>%
    mutate(UNDERC_ID = paste0('UNDERC_', lakeID)) %>%
    dplyr::select(DateTime = dateTimeSample, depth = depthTop, temp, UNDERC_ID, id = sampleID) %>%
    saveRDS(file = outfile)


  sc_indicate(ind_file = outind, data_file = outfile)
}
