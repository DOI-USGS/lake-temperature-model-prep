#' Get an inventory of the number of records per site/variable combination in
#' NWIS
#'
#' Depending on the size of the request, calls to NWIS may need to be
#' partitioned based on record size. This gets the inventory of data available
#' on NWIS that we need to fullfil our "needs" - which is a series of
#' site/variable combinations.
#'
#' @param wqp_needs A dataframe with columns site and variable which describes
#'   what we going to pull from NWIS, which was calculated as the difference
#'   between our data "wants" and our data "haves".
#' @param wqp_variables List of lists that contain parameters of interest (e.g.,
#'   temperature) and all corresponding synonyms available in NWIS (e.g.,
#'   "Temperature" and "Temperature, water").
#' @return A dataframe returned by the function dataRetrieval::whatWQPdata, with
#'   one row per site/variable combination and the 'resultCount' being the
#'   variable from which we will make decisions about partitioning data pull
#'   requests.

inventory_wqp <- function(wqp_needs, wqp_variables) {

  # identify constituents we need
  constituents <- as.character(unique(wqp_needs$variable))

  # loop over the constituents, getting rows for each
  sample_time <- system.time({
    samples <- bind_rows(lapply(constituents, function(constituent) {
      message(Sys.time(), ': getting inventory for ', constituent)
      wqp_args <- list(
        characteristicName = wqp_variables$characteristicName[[constituent]],
        siteid = wqp_needs$site[wqp_needs$variable %in% constituent]
      )
      tryCatch({
        wqp_wdat <- do.call(whatWQPdata, wqp_args)
        mutate(wqp_wdat, constituent=constituent)
      }, error=function(e) {
        # keep going IFF the only error was that there weren't any matching sites
        if(grepl('arguments imply differing number of rows', e$message)) {
          NULL
        } else {
          stop(e)
        }
      })
    }))
  })
  message(sprintf('sample inventory complete, required %0.0f seconds', sample_time[['elapsed']]))

  return(samples)
}

#' Partition calls to WQP based on number of records available in WQP and a
#' number of records that is a reasonable call to WQP.
#'
#' @param wqp_inventory A dataframe with WQP record counts (e.g., the output of
#'   dataRetrieval::whatWQPdata).
#' @param wqp_nrecords_chunk The maximum number of records that should be in a
#'   single call to WQP.
#' @param spatial_crosswalk a table of site crosswalk information including
#'   columns for MonitoringLocationIdentifier and NHDComID
#' @return A dataframe with the same number of rows in wqp_needs - e.g., for
#'   each site/variable combination. The dataframe stores the number of
#'   observations for that site/variable in WQP and the unique task identifier
#'   that partitions site/variables into WQP pulls.
partition_wqp_inventory <- function(wqp_inventory, wqp_nrecords_chunk, spatial_crosswalk) {
  partitions <- bind_rows(lapply(unique(wqp_inventory$constituent), function(constit) {

    # Subset to those rows of the inventory relevant to this constituent
    constit_inventory <- wqp_inventory %>%
      filter(constituent == constit) %>%
      select(MonitoringLocationIdentifier, resultCount) %>%
      left_join(spatial_crosswalk, by='MonitoringLocationIdentifier')

    # Define the atomic_groups to use in setting up data pull partitions. An
    # atomic group is a combination of parameters that can't be reasonably split
    # into multiple WQP pulls. For now we're defining the atomic groups as
    # distinct combinations of constituent (a group of characteristicNames) and
    # site ID (MonitoringLocationIdentifier); however, once we have the
    # crosswalk available at this step, we'll instead use the unique NHD ComID
    # for each lake to ensure all sites from each lake get into the same file
    atomic_groups <- constit_inventory %>%
      group_by(NHDComID) %>%
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
    n_single_site_partitions <- filter(atomic_groups, LakeNumObs >= wqp_nrecords_chunk) %>% nrow()
    n_multi_site_partitions <- filter(atomic_groups, LakeNumObs < wqp_nrecords_chunk) %>%
      pull(LakeNumObs) %>%
      sum() %>%
      { ceiling(./wqp_nrecords_chunk) }
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
        Constituent = constit,
        PullTask = sprintf('%s_%s_%03d', constit, pull_id, assignments)) %>%
      left_join(select(constit_inventory, NHDComID, MonitoringLocationIdentifier, SiteNumObs=resultCount), by='NHDComID') %>%
      select(NHDComID, LakeNumObs, MonitoringLocationIdentifier, SiteNumObs, PullTask, PullDate, Constituent)

    # As a side effect, append this data_frame to an archive file that records
    # all planned partitions. We will likely end up with too many such files
    # while testing the partitioning/pulling code, but we'd rather have too many
    # than risk having to dig through the data files to recover a mapping of
    # which sites have data in which files.
    side_effect_ind <- '6_temp_wqp_fetch/inout/wqp_pull_partitions_archive.feather.ind'
    side_effect_file <- sc_retrieve(side_effect_ind)
    partitions_archive <- feather::read_feather(side_effect_file)
    partitions_all <- bind_rows(partitions_archive, partitions_all)
    feather::write_feather(partitions_all, side_effect_file)
    gd_put(side_effect_ind, side_effect_file)

    # Also write the data_frame to a location that will get overwritten with each new pass through this
    return(partitions)

  }))
}

initialize_partitions_archive <- function(archive_ind) {
  # only write the file if it doesn't already exist
  if(file.exists(archive_ind)) return()

  empty_archive <- data_frame(PullTask='initialization') %>% filter(FALSE)
  feather::
  archive_file <- scipiper::as_data_file(archive_ind)
  gd_put(archive_ind, archive_file)
}
