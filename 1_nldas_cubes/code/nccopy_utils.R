nccopy_nldas <- function(nc_filepath){
  
  nc_filename <- basename(nc_filepath)
  
  nldas_url <- nldas_url_from_file(nc_filename)
  message(nldas_url)
  output <- system(sprintf("nccopy -w %s %s", nldas_url, nc_filepath))
  
  if(output | file.size(nc_filename) == 0){
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
  time_range <- parse_nc_filename(nc_filename, 'time')
  return(seq(time_range[1], time_range[2]))
}

nldas_url_from_file <- function(nc_filename){
  file_chunks <- strsplit(nc_filename,'[_]')[[1]]
  lat_i <- write_grid(x = parse_nc_filename(nc_filename, 'y'))
  lon_i <- write_grid(x = parse_nc_filename(nc_filename, 'x'))
  time_i <- write_grid(x = parse_nc_filename(nc_filename, 'time'))
  var <- parse_nc_filename(nc_filename, 'var')
  sprintf('http://hydro1.sci.gsfc.nasa.gov/dods/NLDAS_FORA0125_H.002?lon%s,time%s,lat%s,%s%s%s%s', lon_i, time_i, lat_i, var, time_i, lat_i, lon_i)
}

write_grid <- function(x){
  sprintf("[%1.0f:1:%1.0f]",x[1],x[2])
}

