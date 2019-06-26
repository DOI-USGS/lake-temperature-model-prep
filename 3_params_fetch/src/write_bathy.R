save_bathy <- function(out_ind, bathy_list){

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(bathy_list, data_file)
  gd_put(out_ind, data_file)

}
