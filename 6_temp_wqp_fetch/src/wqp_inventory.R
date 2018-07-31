#' Get an inventory of the number of records per site/variable combination in NWIS
#'
#' Depending on the size of the request, calls to NWIS may need to be partitioned based 
#' on record size. This gets the inventory of data available on NWIS that we need to fullfil 
#' our "needs" - which is a series of site/variable combinations. 
#' 
#' @param wqp_needs A dataframe with columns site and variable which describes what we going to 
#' pull from NWIS, which was calculated as the difference between our data "wants" and our data 
#' "haves".
#' @param wqp_variables List of lists that contain parameters of interest (e.g., temperature) and 
#' all corresponding synonyms available in NWIS (e.g., "Temperature" and "Temperature, water").
#' @return A dataframe returned by the function dataRetrieval::whatWQPdata, with one row per
#'  site/variable combination and the 'resultCount' being the variable from which we will make 
#'  decisions about partitioning data pull requests.

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

#' Partition calls to WQP based on number of records available in WQP and a number of records that
#' is a reasonable call to WQP.
#' 
#' @param wqp_inventory A dataframe with WQP record counts (e.g., the output of dataRetrieval::whatWQPdata). 
#' @param wqp_nrecords_chunk The maximum number of records that should be in a single call to WQP.
#' @return A dataframe with the same number of rows in wqp_needs - e.g., for each site/variable combination.
#' The dataframe stores the number of observations for that site/variable in WQP and the unique task 
#' identifier that partitions site/variables into WQP pulls. 
partition_wqp_inventory <- function(wqp_inventory, wqp_nrecords_chunk) {
  partitions <- bind_rows(lapply(unique(wqp_inventory$constituent), function(temp_constituent) {
    # an atomic group is a combination of parameters that can't be reasonably
    # split into multiple WQP pulls - in this case we're defining atomic
    # groups as distinct combinations of constituent (a group of
    # characteristicNames) and site ID. 
    
    # right now, just grouping by unique site ID (MonitoringLocationIdentifier), however, 
    # if we have crosswalk available at this step,
    # could put in the unique NHDID for each lake to ensure each lake gets in same file
    atomic_groups <-  wqp_inventory %>%
      filter(constituent == temp_constituent) %>%
      group_by(MonitoringLocationIdentifier) %>%
      summarize(NumObs=sum(resultCount)) %>%
      arrange(desc(NumObs))
    
    # split the full pull (combine atomic groups) into right-sized partitions
    # by an old but fairly effective paritioning heuristic: pick the number of
    # partitions desired, sort the atomic groups by descending size, and then
    # go down the list, each time adding the next atomic group to the
    # partition that's currently smallest
    
    # first, pull out sites that by themselves are larger than the cutoff number
    # for these sites, we will "break" the chunk rule, and get in a single pull from WQP
    single_site_partitions <- filter(atomic_groups, NumObs >= wqp_nrecords_chunk)
    n_single_site_partitions <- nrow(single_site_partitions)
    
    multi_site_partitions <- filter(atomic_groups, NumObs < wqp_nrecords_chunk)
    
    num_partitions <- ceiling(sum(multi_site_partitions$NumObs) / wqp_nrecords_chunk)
    
    partition_sizes <- rep(0, num_partitions)
    assignments <- rep(0, nrow(multi_site_partitions))
    for(i in seq_len(nrow(multi_site_partitions))) {
      size_i <- multi_site_partitions[[i,"NumObs"]]
      smallest_partition <- which.min(partition_sizes)
      assignments[i] <- smallest_partition
      partition_sizes[smallest_partition] <- partition_sizes[smallest_partition] + size_i
    }
    
    last_assignment <- max(assignments)
    
    single_site_partitions <- single_site_partitions %>%
      mutate(
        constituent=temp_constituent,
        PullTask=sprintf('%s_%s_%03d', 'WQP', constituent, seq(last_assignment + 1, last_assignment + nrow(single_site_partitions))))
    
    # create a filename column
    
    multi_site_partitions <- multi_site_partitions %>%
      mutate(
        constituent=temp_constituent,
        PullTask=sprintf('%s_%s_%03d', 'WQP', constituent, assignments))
    
    partitions <- bind_rows(multi_site_partitions, single_site_partitions)
    
    return(partitions)
    
  }))
}
