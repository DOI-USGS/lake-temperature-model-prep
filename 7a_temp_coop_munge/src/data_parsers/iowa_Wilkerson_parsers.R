
parse_2017_Iowa_Lake_Profiles <- function(inind, outind) {
  parse_Iowa_Lake_Profiles(inind, outind)
}
parse_2018_Iowa_Lake_Profiles <- function(inind, outind) {
  parse_Iowa_Lake_Profiles(inind, outind)
}
parse_2019_Iowa_Lake_Profiles <- function(inind, outind) {
  parse_Iowa_Lake_Profiles(inind, outind)
}

parse_Iowa_Lake_Profiles <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  readr::read_csv(infile) %>%
    dplyr::mutate(DateTime = as.Date(Date, format = '%m/%d/%Y'),
                  Iowa_ID = sprintf("Iowa_%s", LakeID)) %>%
    dplyr::select(DateTime, depth = Depth_m, temp = Temp_C, Iowa_ID) %>%
    dplyr::arrange(DateTime, depth) %>%
    dplyr::filter(!is.na(depth), !is.na(temp)) %>%
    saveRDS(file = outfile)

  sc_indicate(ind_file = outind, data_file = outfile)
}
