library(dataRetrieval)
library(dplyr)
library(tidyr)
library(tidyverse)

#### function to Retrieve data from USGS (Water data)
####    The function required a site number
####    and data code to know which retrieve from parameter group table.
####          e.g. "00010" = water temperature (Degree Celsius).

retrieve_reservoir_data <- function(site_id, parameter, out_ind) {


  # note that these reservoirs are not in DV yet
  retrieve_data = readNWISuv(siteNumbers = site_id, parameterCd = parameter)

  ## return reservoir data
  saveRDS(retrieve_data, as_data_file(out_ind))
  gd_put(out_ind)
}
