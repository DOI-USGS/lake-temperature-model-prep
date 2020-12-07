library(dataRetrieval)
library(dplyr)

#### function to Retrieve data from USGS (Water data)
####    The function required a site number
####    and data code to know which retrieve from parameter group table.
####          e.g. "00010" = water temperature (Degree Celsius).

retrieve_reservoir_data <- function(site_id, data_code) {
  #, out_ind) {

  retrieve_data = readNWISuv(siteNumbers = site_id, parameterCd = data_code)
  colnames(retrieve_data)[3] = "date_time"
  colnames(retrieve_data)[4] = "tempC_deep"
  colnames(retrieve_data)[6] = "tempC_middle"
  colnames(retrieve_data)[8] = "tempC_shallow"

  ## return reservoir data
  #saveRDS(retrieve_data, as_data_file(out_ind))
  #gd_put(out_ind)
  return(retrieve_data)
}


#### Using the "retrieve_reservoir_data" function to retrieve the Pepcaton and Cannonsville reservoir data:
pepacton_data_ret <- retrieve_reservoir_data(site_id = "01414750",
                                             data_code = "00010")

cannonsville_data_ret <- retrieve_reservoir_data(site_id = "01423910",
                                                 data_code = "00010")
