

#' Partition calls to WQP based on number of records available in WQP and a
#' number of records that is a reasonable call to WQP.
#'
#' @param partitions_ind Filename of the partitions indicator file to create.
#' @param inventory_ind .ind filename of a table with WQP record counts (from
#'   the output of dataRetrieval::whatWQPdata).
#' @param wqp_charname a vector of characteristic names to be used for the data pull
#' @param wqp_partition_config YAML file containing an element called
#'   $target_pull_size, giving the maximum number of records that should be in a
#'   single call to WQP.
#' @return Nothing useful is returned; however, this function (1) Writes to
#'   partitions_ind a table having the same number of rows as wqp_needs - e.g.,
#'   for each site/variable combination. The dataframe stores the number of
#'   observations for that site/variable in WQP and the unique task identifier
#'   that partitions site/variables into WQP pulls.
partition_wqp_inventory <- function(partitions_ind, inventory_ind, wqp_charnames, wqp_partition_cfg) {
  # Read in the inventory, crosswalk & config

  constituent_name <- basename(partitions_ind) %>% strsplit('[_]') %>% .[[1]] %>% head(1)
  wqp_inventory <- readRDS(sc_retrieve(inventory_ind))

  wqp_partition_config <- yaml::yaml.load_file(wqp_partition_cfg)


  # Define the atomic_groups to use in setting up data pull partitions. An
  # atomic group is a combination of parameters that can't be reasonably split
  # into multiple WQP pulls. We're defining the atomic groups as distinct
  # combinations of constituent (a group of characteristicNames) and NHD-based
  # site ID
  atomic_groups <- wqp_inventory %>%
    arrange(desc(resultCount))

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
  n_single_site_partitions <-
    filter(atomic_groups, resultCount >= target_pull_size) %>% nrow()

  n_multi_site_partitions <-
    filter(atomic_groups, resultCount < target_pull_size) %>%
    pull(resultCount) %>%
    {
      ceiling(sum(.) / target_pull_size)
    }
  num_partitions <-
    n_single_site_partitions + n_multi_site_partitions

  # Assign each site (or lake) to a partition. Sites/lakes with huge numbers
  # of observations will each get their own partition.
  partition_sizes <- rep(0, num_partitions)
  assignments <-
    rep(NA, nrow(atomic_groups)) # use a vector rather than adding a col to atomic_groups b/c it'll be way faster
  for (i in seq_len(nrow(atomic_groups))) {
    smallest_partition <- which.min(partition_sizes)
    assignments[i] <- smallest_partition
    size_i <- atomic_groups[[i, "resultCount"]]
    partition_sizes[smallest_partition] <-
      partition_sizes[smallest_partition] + size_i
  }


  # Prepare one data_frame containing info about each site and lake, including
  # the pull, constituent, and task name (where task name will become the core
  # of the filename)

  partitions <- atomic_groups %>%
    mutate(
      PullTask = sprintf('%s_%03d', constituent_name, assignments)
    ) %>%
    dplyr::select(
      site_id,
      resultCount,
      MonitoringLocationIdentifier,
      PullTask
    )

  # write the data_frame to a location that will get overwritten with
  # each new pass through this function
  feather::write_feather(partitions, scipiper::as_data_file(partitions_ind))
  gd_put(partitions_ind) # 1-arg version requires scipiper 0.0.11+
}
