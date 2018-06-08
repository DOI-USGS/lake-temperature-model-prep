nccopy_nldas <- function(nc_filepath){
  
  nc_filename <- basename(nc_filepath)
  
  nldas_url <- nldas_url_from_file(nc_filename)
  message(nldas_url)
  output <- system(sprintf("nccopy -w %s %s", nldas_url, nc_filepath))
  
  if(output){
    unlink(nc_filename)
    stop(nldas_url, ' **FAILED nccopy**', call. = FALSE)
  }
  # check/verify file has the appropriate number of timesteps...fail if not
  expected_steps <- length(nldas_steps_from_file(nc_filename))
  nc <- ncdf4::nc_open( nc_filepath )
  nc_steps <- nc$dim$time$len
  ncdf4::nc_close(nc)
  if (nc_steps != expected_steps){
    unlink(nc_filename)
    stop('incomplete file ', nc_filename , '\nexpected ', 
         expected_steps, ' steps, found ', nc_steps, ' steps\nfor ', nldas_url, call. = FALSE)
  }
  invisible()
}

nldas_steps_from_file <- function(nc_filename){
  file_chunks <- strsplit(nc_filename,'[_]')[[1]]
  time_range <- strsplit(file_chunks[2],'[.]')[[1]] %>% as.numeric
  return(seq(time_range[1], time_range[2]))
}

nldas_url_from_file <- function(nc_filename){
  file_chunks <- strsplit(nc_filename,'[_]')[[1]]
  lat_i <- write_grid(file_chunks[3])
  lon_i <- write_grid(file_chunks[4])
  time_i <- write_grid(file_chunks[2])
  var <- strsplit(file_chunks[5],'[.]')[[1]][1]
  sprintf('http://hydro1.sci.gsfc.nasa.gov/dods/NLDAS_FORA0125_H.002?lon%s,time%s,lat%s,%s%s%s%s', lon_i, time_i, lat_i, var, time_i, lat_i, lon_i)
}

write_grid <- function(x){
  v <- strsplit(x,'[.]')[[1]]
  sprintf("[%s:1:%s]",v[1],v[2])
}

