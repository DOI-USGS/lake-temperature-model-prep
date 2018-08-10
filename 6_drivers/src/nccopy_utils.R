#' takes a single .nc file target and splits it up into one or more
#' opendap requests, then combines them into the single output target
#' after all small requests are complete

#'@ param nc_filepath the output file path, created using `create_nc_filename`
#'@ param max_steps the maximum number of OPeNDAP timesteps to use for each call

#'@ return the file handle

nccopy_split_combine <- function(nc_filepath, max_steps = 100){
  nc_filename <- basename(nc_filepath)

  time_range <- parse_nc_filename(nc_filename, 'time')
  x <- parse_nc_filename(nc_filename, 'x')
  y <- parse_nc_filename(nc_filename, 'y')
  var <- parse_nc_filename(nc_filename, 'var')
  t0 <- seq(time_range[1], to = time_range[2], by = max_steps)
  if(tail(t0, 1L) == time_range[2]){ # the somewhat rare case where the time start sequence includes the final step too
    t0 <- head(t0, -1L)
  }
  t1 <- c(tail(t0, -1L) -1, time_range[2])

  temp_nc_dir <- file.path(tempdir(), '.nc_split_files')
  dir.create(temp_nc_dir)
  # delete all temporary split files and their directory:
  on.exit(unlink(temp_nc_dir, recursive = TRUE))

  split_file_metadata <- sapply(1:length(t0), function(i) {
    create_nc_filename(t0[i], t1[i], x0 = x[1], x1 = x[2], y0 = y[1], y1 = y[2], variable = var)
  }, USE.NAMES = FALSE)

  split_file_info <- data.frame(file_meta = split_file_metadata, file_num = 1:length(split_file_metadata)) %>%
    mutate(nc_temp_file_unl = file.path(temp_nc_dir, sprintf("NLDAS_%s_unlimited.nc", stringr::str_pad(file_num, width = 3, pad = '0')))) %>%
    mutate(nc_temp_file_fixed = file.path(temp_nc_dir, sprintf("NLDAS_%s_fixed.nc", stringr::str_pad(file_num, width = 3, pad = '0'))))

  pb <- progress_bar$new(
    format = "  nccopy :what [:bar] :percent eta: :eta",
    clear = TRUE, total = length(t0))

  for (split_file in split_file_metadata){

    nc_temp_file_fixed <- split_file_info %>% filter(file_meta == split_file) %>% pull(nc_temp_file_fixed)
    nc_temp_file_unl <- split_file_info %>% filter(file_meta == split_file) %>% pull(nc_temp_file_unl)

    nldas_url <- nldas_url_from_file(split_file)
    nccopy_retry(nldas_url, nc_temp_file_fixed, variable = var)

    # make the time dimension unlimited so we can append to this file:
    system(sprintf("ncks --mk_rec_dmn time %s -o %s", nc_temp_file_fixed, nc_temp_file_unl))

    what_download <- strsplit(nldas_url, split = '[?]')[[1]][2]
    pb$tick(tokens = list(what = what_download))
  }

  split_filepaths <- paste(pull(split_file_info, nc_temp_file_unl), collapse = ' ')

  combine_nc_files(split_filepaths, nc_filepath)

  # check for timestep order here??

  # check that the resulting file has the appropriate number of timesteps:
  verify_nc_output(nc_filepath)

  invisible(nc_filepath)
}


#' take a bunch of nc files that share the same variable and combine them into a single file
#' needs to remove time as a record dimension w/ ncks
combine_nc_files <- function(filepaths_to_combine, nc_out_filepath){

  nc_unl_filepath <- file.path(tempdir(), "NLDAS_COMBINED_unl.nc")
  # need NCO to run this. Combine all split files into one:
  system(sprintf('ncrcat -h --overwrite %s %s', filepaths_to_combine, nc_unl_filepath))


  if (file.exists(nc_out_filepath)){
    unlink(nc_out_filepath) # doesn't seem to honor the overwrite arg and gets hung up on interactive mode
  }

  # then make the time dim fixed again on the larger file
  system(sprintf("ncks --fix_rec_dmn time %s -o %s", nc_unl_filepath, nc_out_filepath))

  # clean up
  unlink(nc_unl_filepath)
  invisible(nc_out_filepath)
}

#' fault tolerate OPeNDAP request
nccopy_retry <- function(url, file_out, retries = 15, verbose = FALSE, snooze = 5, variable){
  retry <- 0
  while (retry < retries){
    output <- system(sprintf("nccopy -w  %s %s", url, file_out), ignore.stdout = TRUE, ignore.stderr = TRUE)

    if(output | file.size(file_out) == 0){
      unlink(file_out)
      retry <- retry + 1
      Sys.sleep(snooze)
      if (verbose) message('Zero file, retry:', retry)
    } else if (!verify_nc_clean(file_out, variable = variable)){
      unlink(file_out)
      retry <- retry + 1
      if (verbose) message('Bad file banding, retry:', retry)
    } else {
      if (verbose) message('success! after retry:', retry)
      break
    }
  }
  if (retry < retries){
    invisible(file_out)
  } else {
    stop('\nexceded retries for nccopy', call. = FALSE)
  }
}

verify_nc_clean <- function(nc_filepath, variable){
  # odd issue w/ zero data "banding" on junk files


  nc <- ncdf4::nc_open(nc_filepath)
  x_max <- length(ncvar_get(nc, 'lon'))
  # read all x, one y, one time:
  first_band <- ncvar_get(nc, varid = variable, start = c(1, 1, 1), count = c(-1, 1, 1))
  ncdf4::nc_close(nc)
  if (all(first_band[seq(2, x_max, 2)] < 0) & all(first_band[seq(1, x_max, 2)] == 0)){
    return(FALSE)
  } else {
    return(TRUE)
  }
}

#' check that the .nc file output has the right number of timesteps
verify_nc_output <- function(nc_filepath){
  nc_filename <- basename(nc_filepath)
  expected_steps <- length(nldas_steps_from_file(nc_filename))
  nc <- ncdf4::nc_open(nc_filepath)
  time <- ncvar_get(nc, 'time')
  ncdf4::nc_close(nc)
  if (!all(time == cummax(time))){
    stop('time is not monotonically increasing in ', nc_filename, call. = FALSE)
  }
  nc_steps <- length(time)

  if (nc_steps != expected_steps){
    unlink(nc_temp_combined_file)
    stop('incomplete file ', nc_filename , '\nexpected ',
         expected_steps, ' steps, found ', nc_steps, ' steps', call. = FALSE)
  }
}

nldas_steps_from_file <- function(nc_filename){
  time_range <- parse_nc_filename(nc_filename, 'time')
  return(seq(time_range[1], time_range[2]))
}

nldas_url_from_file <- function(nc_filename){
  y <- parse_nc_filename(nc_filename, 'y')
  x <- parse_nc_filename(nc_filename, 'x')
  time <- parse_nc_filename(nc_filename, 'time')
  var <- parse_nc_filename(nc_filename, 'var')
  return(create_nldas_url(t0 = time[1], t1 = time[2], x0 = x[1], x1 = x[2], y0 = y[1], y1 = y[2], variable = var))
}

# http://hydro1.sci.gsfc.nasa.gov/dods/NLDAS_FORA0125_H.002?lon[221:1:344],time[0:1:99],lat[132:1:196],apcpsfc[0:1:99][132:1:196][221:1:344]
create_nldas_url <- function(t0, t1, x0, x1, y0, y1, variable){
  base_url <- 'http://hydro1.sci.gsfc.nasa.gov/dods/NLDAS_FORA0125_H.002'
  query_url <- '?lon[%1.0f:1:%1.0f],time[%1.0f:1:%1.0f],lat[%1.0f:1:%1.0f],%s[%1.0f:1:%1.0f][%1.0f:1:%1.0f][%1.0f:1:%1.0f]'
  sprintf(paste0(base_url, query_url), x0, x1, t0, t1, y0, y1, variable, t0, t1, y0, y1, x0, x1)
}



