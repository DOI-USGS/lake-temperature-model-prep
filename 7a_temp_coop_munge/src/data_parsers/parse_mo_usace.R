# NOTES --------------------------------------------
#' This was a challenging dataset due to inconsistencies both within
#' individual workbooks and between workbooks.
#'
#' This family of functions gets the vast majority of data and most
#' major bugs have been resolved except one - some locations need name repair
#' (e.g., station `TC-10` is improperly labeled as `10`). I decided not to
#' resolve this because of the time required to do so. We ultimately lose around
#' ~200 records which is < 1% of the data in the dataset.

# Top level call to individual parsers ---------------
parse_Missouri_USACE_2009_2021 <- function(inind, outind) {
  infile <- sc_retrieve(inind, remake_file = '6_temp_coop_fetch_tasks.yml')
  outfile <- as_data_file(outind)

  # Grab files -------------------------------
  # Unzip the file with all MO USACE data, then cleanup unzipped files
  # which aren't needed externally before leaving the function
  unzip_dir <- tempdir()
  files_from_zip <- unzip(infile, exdir = unzip_dir)
  on.exit(unlink(unzip_dir, recursive = TRUE))

  # Define variables needed to group and parse data -----
  ## Define columns to keep
  keep_cols_most_yrs <- c('Site', 'Date', 'Dep', 'Temp')
  keep_cols_2009 <- c('Site', 'Date', 'Dep', 'Temp', '...9')
  keep_cols_2018 <- c('Site', 'Date', 'Dep', 'Temp', '...4', '...5')

  ## Define which years can be parsed together - this will be used for
  ## a `grep` call when grouping files
  most_yrs <- paste(c(2011:2017, 2019:2021), collapse = '|')

  ## Define sheets that should be removed if encountered
  remove_sheets <- c('Sept', 'apriljune', 'PE_dam', # 2010
                     'July - Inflows', 'August inflows', 'September inflows', # 2011
                     'Sheet1', 'Sheet2', 'Sheet3' # 2012, 2013 & 2014
  )

  # Group files -------------------------
  files_most_yrs <- files_from_zip[grep(most_yrs, files_from_zip)]
  files_2009 <- files_from_zip[grep('2009', files_from_zip)]
  files_2010 <- files_from_zip[grep('2010', files_from_zip)]
  files_2018 <- files_from_zip[grep('2018', files_from_zip)]

  # Parse most data ----------------------------
  dat_most_yrs <- files_most_yrs %>%
    purrr::map_dfr(~ parse_mo_usace(., keep_cols = keep_cols_most_yrs,
                                    remove_sheets = remove_sheets))

  # Parse 2009 ---------------------------------
  dat_2009 <- files_2009 %>%
    purrr::map_dfr(~ parse_mo_usace(., keep_cols = keep_cols_2009,
                                    remove_sheets = remove_sheets))

  # Parse 2010 ---------------------------------
  dat_2010 <- files_2010 %>%
    purrr::map_dfr(~ parse_mo_usace_2010(., keep_cols = keep_cols_most_yrs,
                   subset_sheets = c('PT', 'RA'), subset_skip = 1,
                   remove_sheets = remove_sheets))

  # Parse 2018 ---------------------------------
  dat_2018 <- files_2018 %>%
    purrr::map_dfr(~ parse_mo_usace_2018(., keep_cols = c(1, 2, 4, 5),
                                         remove_sheets = remove_sheets))

  # Aggregate into output ---------------------------
  data_clean <- dplyr::bind_rows(
    dat_most_yrs,
    dat_2009,
    dat_2010,
    dat_2018
  )

  # Light cleaning to match the sampling location xwalk table
  data_clean$site <- gsub(" ", "", data_clean$site)
  data_clean$site <- gsub("-", "", data_clean$site)
  data_clean$site <- toupper(data_clean$site)

  # naming convention clean-up for merging and xwalking
  data_clean <- data_clean %>%
    # normalize site names to match xwalk table in `2_crosswalk_munge/out`
    dplyr::mutate(site = gsub(" ", "", site)) %>%
    dplyr::mutate(site = gsub("-", "", site)) %>%
    dplyr::mutate(site = toupper(site)) %>%
    # normalize column names to match standard naming conventions
    dplyr::rename(DateTime = date) %>%
    # preserve `site` column because there are multiple sites per lake, but
    # add a new column for matching with xwalk table in `2_crosswalk_munge/out`
    dplyr::mutate(mo_usace_id = paste('mo_usace_', site, sep = ''))

  # Save outputs ------------------------------------
  saveRDS(object = data_clean, file = outfile)
  sc_indicate(ind_file = outind, data_file = outfile)
}

# Parser functions -----------------------------------

#' Parsing functions for each group of years
#'
#' @param filepath chr, full file path
#' @param keep_cols chr vector of column names to keep. Uses partial matching
#' @param subset_sheets chr vector, a subset of sheets that may need to skip a few rows to read properly
#' @param subset_skip num, the number of rows that need to be skipped in order to read the files listed in `subset_sheets`
#'
parse_mo_usace <- function(filepath, keep_cols, remove_sheets) {

  # list all sheets in the df and read each one
  dat <- read_usace_sheets(filepath) %>%
    # column name case is inconsistent - picking a convention and sticking with it
    sheet_cols_to_lower(.) %>%

    # a subset of years have extra sheets that are not needed - it was quicker to look them up and kick them out
    remove_extra_sheets(., remove_sheets) %>%

    # Column names are inconsistent bot between workbooks and within workbooks.
    # A little column validation happens here
    validate_mo_usace_col_names(.) %>%

    # Because of the data format, many extra columns come along
    # for the ride. This step limits us to our columns of interest
    select_cols_to_keep(., keep_cols) %>%

    # All datasets have a mix of data and column headers which causes all
    # data to import as `chr`
    coerce_numeric_columns(.) %>%

    # Site IDs are not not backfilled in most of the data sets
    backfill_site_ids(.) %>%

    # Removes all partially populated row created by the
    # previous two munge steps
    remove_incomplete_rows(.) %>%
    convert_date(.) %>%
    dplyr::bind_rows(.)

}

parse_mo_usace_2018 <- function(filepath,
                                keep_cols = c(1, 2, 4, 5), remove_sheets) {

  # list all sheets in the df and read each one
  dat <- read_usace_sheets(filepath) %>%
    # column name case is inconsistent - picking a convention and sticking with it
    sheet_cols_to_lower(.) %>%

    # a subset of years have extra sheets that are not needed - it was quicker to look them up and kick them out
    remove_extra_sheets(., remove_sheets) %>%

    # 2018 requires a hard-coded grab because I don't have a great way to
    # automatically differentiate Depth and Temp. 2018 always requires these cols
    lapply(., function(df){df[ , keep_cols]}) %>%

    # Column names are inconsistent both between workbooks and within workbooks.
    # A little column validation happens here
    lapply(., function(df){
      names(df) <- c('site', 'date', 'depth', 'temp')
      return(df)
    }) %>%

    # All datasets have a mix of data and column headers which causes all
    # data to import as `chr`
    coerce_numeric_columns(.) %>%

    # Site IDs are not not backfilled in most of the data sets
    backfill_site_ids(.) %>%

    # Removes all partially populated row created by the
    # previous two munge steps
    remove_incomplete_rows(.) %>%
    convert_date(.) %>%
    dplyr::bind_rows(.)

}

parse_mo_usace_2010 <- function(filepath, keep_cols,
                                subset_sheets = c('PT', 'RA'),
                                subset_skip = 1, remove_sheets) {

  # list all sheets in the df and read each one
  dat <- read_usace_sheets(filepath, subset_sheets, subset_skip) %>%
    # column name case is inconsistent - picking a convention and sticking with it
    sheet_cols_to_lower(.) %>%

    # a subset of years have extra sheets that are not needed - it was quicker to look them up and kick them out
    remove_extra_sheets(., remove_sheets) %>%

    # Column names are inconsistent bot between workbooks and within workbooks.
    # A little column validation happens here
    validate_mo_usace_col_names(.) %>%

    # Because of the data format, many extra columns come along
    # for the ride. This step limits us to our columns of interest
    select_cols_to_keep(., keep_cols) %>%

    # All datasets have a mix of data and column headers which causes all
    # data to import as `chr`
    coerce_numeric_columns(.) %>%

    # Site IDs are not not backfilled in most of the data sets
    backfill_site_ids(.) %>%

    # Removes all partially populated row created by the
    # previous two munge steps
    remove_incomplete_rows(.) %>%
    convert_date(.) %>%
    dplyr::bind_rows(.)

}


# Helper functions -----------------------------------

#' Read data in from all sheets in an excel file
#'
#' This function includes a bit of error handling to manage the following cases:
#' (1) If the excel workbook contains empty sheets
#' (2) If some of the files have problematic data in the first row and need to be skipped
#'
#' @param filepath chr, full file path
#' @param subset_sheets chr vector, a subset of sheets that may need to skip a few rows to read properly
#' @param subset_skip num, the number of rows that need to be skipped in order to read the files listed in `subset_sheets`
#'
read_usace_sheets <- function(filepath, subset_sheets = NULL, subset_skip = NULL) {

  # read in data
  dat <- filepath %>%
    readxl::excel_sheets() %>% # get all sheet names for `read_xlsx`
    purrr::set_names() %>% # name the data.frames in the list - skip?
    purrr::map(read_xlsx, path = filepath, col_names = T)

  # this functionality it required to read in all 2010 datasets
  if(!is.null(subset_sheets)) {

    # sanity check
    if(is.null(subset_skip)){stop("`subset_skip` must be specified if `subset_sheets` is specified")}

    # remove `subset_sheets` from the data set
    dat <- dat[!(names(dat) %in% subset_sheets)]

    # read in the sheets that have to skip `subset_skip` rows before reading
    dat_subset <- subset_sheets %>%
      purrr::set_names() %>%
      purrr::map(readxl::read_xlsx, path = filepath, skip = subset_skip, col_names = T)

    # join the pieces back together
    dat <- c(dat, dat_subset)
  }

  # remove sheets with no data
  dat <- dat[sapply(dat, function(x) dim(x)[1]) > 0]

  # some sheet names have extra whitespace and inconsistent case usage
  names(dat) <- trimws(names(dat)) %>% toupper()

  return(dat)
}

#' Convert sheet column names to lower case
#'
#' Column names use inconsistent case within and across workbooks.
#' This function resolves this issue.
#'
#' @param ls_df a list of `data.frames` generated by reading in excel sheets
#'
sheet_cols_to_lower <- function(ls_df) {
  lapply(ls_df, function(x){
    names(x) <- tolower(names(x))
    return(x)
  })
}

#' Remove manually identified sheets that are not necessary
#'
#' Several workbooks have superflous sheets that need to be removed.
#' This requires manual identification of extra sheets
#'
#' @param ls_df a list of `data.frames` generated by reading in excel sheets
#' @param remove_sheets `chr` vector of sheet names that should be removed from the list
#'
remove_extra_sheets <- function(ls_df, remove_sheets) {
  # keep elements from `ls_df` that do not match `remove_sheets`
  ls_df[!(names(ls_df) %in% toupper(remove_sheets))]
}

#' Select columns to keep from a list of `data.frames`
#'
#' Select columns to keep in teh `data.frame` based on partial matching.
#' Partial matching is required because column names are variable within
#' each workbook (e.g. "depth" may be any of the following: "Dep100", "Depth (M)", "Dep25", etc)
#'
#' @param ls_df a list of `data.frames` generated by reading in excel sheets
#' @param keep_cols a `chr` vector of column names to keep. Uses partial matching to get around inconsistent naming conventions
#'
select_cols_to_keep <- function(ls_df, keep_cols) {
  keep_cols <- tolower(keep_cols)

  lapply(ls_df, function(x){
    x %>%
      # partial match because names are variable - e.g. "Depth", "Depth (M)", "Dep100", etc
      dplyr::select(contains(keep_cols)) %>%
      # both secchi depth and depth exist in some data sets
      dplyr::select(- contains("Secchi"))
  })
}

#' Coerce numeric columns
#'
#' This function converts user-specified columns to `numeric`. It
#' assumes that the `Site` column is the only `chr` column.
#'
#' @param ls_df a list of `data.frames` generated by reading in excel sheets
#'
coerce_numeric_columns <- function(ls_df) {
  lapply(ls_df, function(df){
    # ID all the numeric columns
    num_cols <- c(1:ncol(df)) %>%
      .[!. %in% grep("site", names(df))] # find and remove site column

    df[num_cols] <- sapply(df[num_cols], as.numeric)

    return(df)
  })
}

#' Validate column names
#'
#' MO USACE datasets have a wide variety of column names. This function
#' checks for inconsistencies and fixes them. this function calls two
#' helper functions: `run_name_checks` and `find_col_idx`
#'
#' @param ls_df a list of `data.frames` generated by reading in excel sheets
#'
validate_mo_usace_col_names <- function(ls_df) {

  df_names <- names(ls_df)

  mapply(run_name_checks, ls_df, site_pattern = df_names, SIMPLIFY = FALSE)
}

#' Column check logic
#'
#' Validate column names and rename to a consistent set of
#' hard-coded names `c('site', 'date', 'temp', 'depth')`
#'
#' @param df data.frame
#' @param site_pattern chr, partial match that should be use to verify if a column is the `site` column.
#'
run_name_checks <- function(df, site_pattern) {

  # grab data.frame names for validation
  chk_names <- names(df)

  # check for the use of "sample id" instead of "site" and reset `chk_names`
  if(!is.na(pmatch('samp', chk_names))){
    names(df)[pmatch('samp', chk_names)] <- 'site'
    chk_names <- names(df)
    }

  # check for "site" column names using a partial match
  if(is.na(pmatch('site', chk_names))) {
    #looks for the name of the data.frame which is typically a two letter abbreviation (e.g. "RA")
    site_col_idx <- find_site_col_idx(df, site_pattern)
    names(df)[site_col_idx] <- 'site'
  } else {
    names(df)[pmatch('sit', names(df))] <- 'site'
  }

  # these other should be there, so just making a stop right now
  # check for "date" column names using a partial match
  if(is.na(pmatch('dat', chk_names))) {
    stop("Date column not found")
  } else {
    names(df)[pmatch('dat', names(df))] <- 'date'
  }

  # check for "depth" column names using a partial match
  if(is.na(pmatch('dep', chk_names))) {
    stop("Depth column not found")
  } else {
    names(df)[pmatch('dep', names(df))] <- 'depth'
  }

  # check for temp column names using a partial match
  if(is.na(pmatch('tem', chk_names))) {
    stop("Temp column not found")
  } else {
    names(df)[pmatch('tem', names(df))] <- 'temp'
  }

  return(df)
}

#' Identify site column index in a data.frame
#'
#' Given a pattern, identify the  position of the site column within a
#' data.frame.
#'
#' @param df a data.frame
#' @param pattern chr, pattern to match. Supports partial matching.
#'
find_site_col_idx <- function(df, pattern) {
  which(apply(df, 2, function(x) any(grepl(pattern, x))))
}

#' Backfill site IDs
#'
#' @param ls_df a list of `data.frames` generated by reading in excel sheets
#'
backfill_site_ids <- function(ls_df){
  lapply(ls_df, function(df) {
    # requires that there be a column called `site`
    df %>% tidyr::fill(site, .direction = 'down')
  })
}

#' Remove incomplete rows
#'
#' @param ls_df a list of `data.frames` generated by reading in excel sheets
#'
remove_incomplete_rows <- function(ls_df) {
  lapply(ls_df, function(df){
    df[complete.cases(df), ]
  })
}

#' Converts Excel serial date to `Date` object
#'
#' @param ls_df a list of `data.frames` generated by reading in excel sheets
#'
convert_date <- function(ls_df) {
  lapply(ls_df, function(x) {
    x$date <- as.Date(x$date, origin = '1899-12-30')
    return(x)
  })
}
