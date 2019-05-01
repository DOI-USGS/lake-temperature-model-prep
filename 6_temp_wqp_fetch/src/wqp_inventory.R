#' Get an inventory of the number of records per site/variable combination in
#' WQP
#'
#' Depending on the size of the request, calls to WQP may need to be
#' partitioned based on record size. This gets the inventory of data available
#' on WQP that we need to fullfil our "needs" - which is a series of
#' site/variable combinations.
#'
#' @param needs_ind Indicator file for a table with columns site and variable
#'   which describes what we going to pull from WQP, which was calculated as
#'   the difference between our data "wants" and our data "haves".
#' @param wqp_pull_params List of lists that contain parameters of interest
#'   (e.g., temperature) and all corresponding synonyms available in WQP (e.g.,
#'   "Temperature" and "Temperature, water"), plus other pull parameters.
#' @param wqp_partition_config configuration information for the data/inventory
#'   pulls
#' @return A dataframe returned by the function dataRetrieval::whatWQPdata, with
#'   one row per site/variable combination and the 'resultCount' being the
#'   variable from which we will make decisions about partitioning data pull
#'   requests.
inventory_wqp <- function(inv_ind, needs_ind, wqp_pull_params, wqp_partition_cfg) {

  # identify constituents we need
  needs <- feather::read_feather(sc_retrieve(needs_ind))
  constituents <- as.character(unique(needs$ParamGroup))

  # get pull configuration information
  wqp_partition_config <- yaml::yaml.load_file(wqp_partition_cfg)
  max_inv_chunk <- wqp_partition_config$target_inv_size

  # loop over the constituents and groups of sites, getting rows for each
  total_time <- system.time({
    samples <- if(length(constituents) > 0) {
      bind_rows(lapply(constituents, function(constituent) {
        wqp_args <- wqp_pull_params
        wqp_args$characteristicName <- wqp_pull_params$characteristicName[[constituent]]
        constit_sites <- needs %>%
          filter(ParamGroup %in% constituent) %>%
          pull(MonitoringLocationIdentifier)
        n_chunks <- ceiling(length(constit_sites)/max_inv_chunk)
        bind_rows(lapply(seq_len(n_chunks), function(chunk) {
          chunk_sites <- (1 + (chunk-1)*max_inv_chunk) : min(length(constit_sites), chunk*max_inv_chunk)
          message(sprintf(
            '%s: getting inventory for %s, sites %d-%d of %d...',
            Sys.time(), constituent, head(chunk_sites, 1), tail(chunk_sites, 1), length(constit_sites)),
            appendLF=FALSE)
          wqp_args <- c(wqp_args, list(siteid = constit_sites[chunk_sites]))
          call_time <- system.time({
            wqp_wdat <- tryCatch({
              do.call(whatWQPdata, wqp_args) %>%
                dplyr::select(MonitoringLocationIdentifier, MonitoringLocationName, resultCount)
            }, error=function(e) {
              # keep going IFF the only error was that there weren't any matching sites
              if(grepl('arguments imply differing number of rows', e$message)) {
                data_frame(MonitoringLocationIdentifier='', MonitoringLocationName='', resultCount=0) %>%
                  filter(FALSE)
              } else {
                stop(e)
              }
            })
          })
          message(sprintf('retrieved %d rows in %0.0f seconds', nrow(wqp_wdat), call_time[['elapsed']]))
          return(wqp_wdat)
        })) %>%
          mutate(ParamGroup = constituent)
      })) %>%
        right_join(needs, by=c('ParamGroup', 'MonitoringLocationIdentifier')) %>%
        mutate(resultCount=as.integer(case_when(is.na(resultCount) ~ 0, TRUE ~ resultCount)))
    } else {
      data_frame(MonitoringLocationIdentifier='', MonitoringLocationName='', resultCount=1L, ParamGroup='', site_id='') %>%
        filter(FALSE)
    }
  })
  message(sprintf('sample inventory complete, required %0.0f seconds', total_time[['elapsed']]))

  # write and indicate the data file
  data_file <- scipiper::as_data_file(inv_ind)
  feather::write_feather(samples, data_file)
  sc_indicate(inv_ind, data_file=data_file)
}

#' Partition calls to WQP based on number of records available in WQP and a
#' number of records that is a reasonable call to WQP.
#'
#' @param partitions_ind Filename of the partitions indicator file to create.
#' @param inventory_ind .ind filename of a table with WQP record counts (from
#'   the output of dataRetrieval::whatWQPdata).
#' @param wqp_partition_config YAML file containing an element called
#'   $target_pull_size, giving the maximum number of records that should be in a
#'   single call to WQP.
#' @param archive_ind Name of the indicator file for the current archive of
#'   partitions information, which will be modified as a side effect of this
#'   function
#' @return Nothing useful is returned; however, this function (1) Writes to
#'   partitions_ind a table having the same number of rows as wqp_needs - e.g.,
#'   for each site/variable combination. The dataframe stores the number of
#'   observations for that site/variable in WQP and the unique task identifier
#'   that partitions site/variables into WQP pulls.
partition_wqp_inventory <- function(partitions_ind, inventory_ind, wqp_partition_cfg, archive_ind) {
  # Read in the inventory, crosswalk & config
  wqp_inventory <- feather::read_feather(sc_retrieve(inventory_ind))
  wqp_partition_config <- yaml::yaml.load_file(wqp_partition_cfg)

  partitions <- bind_rows(lapply(unique(wqp_inventory$ParamGroup), function(constituent) {

    # Filter the inventory to rows for this constituent
    constit_inventory <- wqp_inventory %>%
      filter(ParamGroup == constituent)

    # Define the atomic_groups to use in setting up data pull partitions. An
    # atomic group is a combination of parameters that can't be reasonably split
    # into multiple WQP pulls. We're defining the atomic groups as distinct
    # combinations of constituent (a group of characteristicNames) and NHD-based
    # site ID
    atomic_groups <- constit_inventory %>%
      group_by(site_id) %>%
      summarize(LakeNumObs=sum(resultCount)) %>%
      arrange(desc(LakeNumObs))

    # Partition the full pull into sets of atomic groups that form right-sized
    # partitions. Use an old but fairly effective paritioning heuristic: pick
    # the number of partitions desired, sort the atomic groups by descending
    # size, and then go down the list, each time adding the next atomic group to
    # the partition that's currently smallest. With this approach we can balance
    # the distribution of data across partitions while ensuring that each site's
    # observations are completely contained within one file.

    # Decide how many partitions to create. This will be (A) the number of sites
    # (or lakes) with more observations than the target partition size, because
    # each of these sites/lakes will get its own partition + (B) the number of
    # remaining observations divided by the target partition size.
    target_pull_size <- wqp_partition_config$target_pull_size
    n_single_site_partitions <- filter(atomic_groups, LakeNumObs >= target_pull_size) %>% nrow()
    n_multi_site_partitions <- filter(atomic_groups, LakeNumObs < target_pull_size) %>%
      pull(LakeNumObs) %>%
      { ceiling(sum(.)/target_pull_size) }
    num_partitions <- n_single_site_partitions + n_multi_site_partitions

    # Assign each site (or lake) to a partition. Sites/lakes with huge numbers
    # of observations will each get their own partition.
    partition_sizes <- rep(0, num_partitions)
    assignments <- rep(NA, nrow(atomic_groups)) # use a vector rather than adding a col to atomic_groups b/c it'll be way faster
    for(i in seq_len(nrow(atomic_groups))) {
      smallest_partition <- which.min(partition_sizes)
      assignments[i] <- smallest_partition
      size_i <- atomic_groups[[i,"LakeNumObs"]]
      partition_sizes[smallest_partition] <- partition_sizes[smallest_partition] + size_i
    }

    # Prepare one data_frame containing info about each site and lake, including
    # the pull, constituent, and task name (where task name will become the core
    # of the filename)
    pull_time <- Sys.time()
    attr(pull_time, 'tzone') <- 'UTC'
    pull_id <- format(pull_time, '%y%m%d%H%M%S')
    partitions <- atomic_groups %>%
      mutate(
        PullDate = pull_time,
        ParamGroup = constituent,
        PullTask = sprintf('%s_%s_%03d', constituent, pull_id, assignments)) %>%
      left_join(dplyr::select(constit_inventory, site_id, MonitoringLocationIdentifier, SiteNumObs=resultCount), by='site_id') %>%
      dplyr::select(site_id, LakeNumObs, MonitoringLocationIdentifier, SiteNumObs, PullTask, PullDate, ParamGroup)

    return(partitions)
  }))

  # As a side effect, append this data_frame to an archive file that records
  # all planned partitions. We will likely end up with too many such files
  # while testing the partitioning/pulling code, but we'd rather have too many
  # than risk having to dig through the data files to recover a mapping of
  # which sites have data in which files.
  archive_file <- sc_retrieve(archive_ind)
  partitions_archive <- feather::read_feather(archive_file)
  partitions_all <- bind_rows(partitions_archive, partitions)
  feather::write_feather(partitions_all, archive_file)
  gd_put(archive_ind) # 1-arg version requires scipiper 0.0.11+

  # Also write the data_frame to a location that will get overwritten with
  # each new pass through this function
  feather::write_feather(partitions, scipiper::as_data_file(partitions_ind))
  gd_put(partitions_ind) # 1-arg version requires scipiper 0.0.11+
}

#' Create and push an empty partitions archive if needed. If a partitions
#' archive already exists, this function does nothing.
#'
#' @param archive_ind the .ind file for the desired partitions archive feather
#'   file
initialize_partitions_archive <- function(archive_ind) {
  # only write the file if it doesn't already exist
  if(file.exists(archive_ind)) {
    message('partitions archive already exists; skipping initialization')
    return()
  }

  empty_archive <-
    data_frame(
      site_id='',
      LakeNumObs=0L,
      MonitoringLocationIdentifier='',
      SiteNumObs=0L,
      PullTask='',
      PullDate=Sys.time(),
      ParamGroup='') %>%
    filter(FALSE)
  archive_file <- scipiper::as_data_file(archive_ind)
  feather::write_feather(empty_archive, path=archive_file)
  gd_put(archive_ind, archive_file)
}

# not ready for prime time:
#' #' This function can be manually run every so often to clean out partitions
#' #' records for data pulls that never happened. When running this, be careful to ...
#' clean_partitions_archive <- function(archive_ind, partitions_file) {
#'
#'   # read in the current (pending) partitions file and ...
#'   partitions_current <- feather::read_feather(partitions_file)
#'
#'   # retrieve and read in the archive file
#'   archive_file <- sc_retrieve(archive_ind)
#'   partitions_archive <- feather::read_feather(archive_file)
#'
#'   # clean up the archive file by comparing to ...
#'
#'
#'   # save and push the archive file back to the shared cache
#'   feather::write_feather(partitions_all, archive_file)
#'   gd_put(archive_ind, archive_file)
#'
#' }
