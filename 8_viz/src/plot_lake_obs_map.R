library(tidyverse)
library(httr)
library(sf)

fetch_zip_url_sf <- function(zip_url, layer_name){

  destination = tempfile(pattern = layer_name, fileext='.zip')
  file <- httr::GET(zip_url, httr::write_disk(destination, overwrite=T), httr::progress())
  shp_path <- tempdir()
  unzip(destination, exdir = shp_path)

  sf::st_read(shp_path, layer=layer_name) %>%
    sf::st_transform(crs = 4326) %>%
    mutate(state = dataRetrieval::stateCdLookup(STATEFP)) %>%
    dplyr::select(state, county = NAME)

}
us_counties_sf <- fetch_zip_url_sf(
  I('https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_county_5m.zip'),
  layer_name = I('cb_2018_us_county_5m'))

plot_lake_obs_map <- function(fileout_base, temp_obs_ind = '7b_temp_merge/out/temp_data_with_sources.feather.ind', lakes_sf_fl, us_counties_sf, plot_crs = "+init=epsg:2811"){

  # Read the data
  temp_data <- feather::read_feather(scipiper::as_data_file(temp_obs_ind)) # replace as_data_file with sc_retrieve when practical

  # Generate tables of relevant counts
  lake_obs_counts <- temp_data %>%
    group_by(site_id, date) %>%
    summarize(n_per_date = n(), .groups='drop_last') %>%
    mutate(n_per_date = replace_na(n_per_date, 0)) %>%
    filter(n_per_date >= 2) %>%
    group_by(site_id) %>%
    summarize(obs_dates = n(), .groups='drop')

  # read in the lake polygons and merge with counts
  all_lakes <- readRDS(lakes_sf_fl) %>%
    st_transform(crs = plot_crs) %>%
    #dplyr::sample_n(1000) %>%
    mutate(area = as.numeric(st_area(Shape))) %>%
    sf::st_simplify(dTolerance = 40) %>%
    left_join(lake_obs_counts, by='site_id') %>%
    mutate(
      obs_dates = ifelse(is.na(obs_dates), 0, obs_dates),
      obs_date_bin = cut(
        obs_dates,
        breaks=c(-1, 10, 100, max(obs_counts$obs_dates)),
        labels=c('0-10', '10-100', sprintf('100-%d', max(obs_counts$obs_dates)))))
        # breaks=c(-1, 0, 10, 100, 1000, max(obs_counts$obs_dates)),
        # labels=c('0', '1-10', '10-100', '100-1000', sprintf('1000-%d', max(obs_counts$obs_dates)))))

  # compute/plot full distribution function
  obs_counts <- all_lakes %>%
    st_set_geometry(NULL) %>%
    group_by(obs_dates, obs_date_bin) %>%
    summarize(n_sites_at = n(), .groups='drop') %>%
    arrange(desc(obs_dates)) %>%
    mutate(n_sites = cumsum(n_sites_at))
  # ggplot(obs_counts, aes(y=n_sites, x=obs_dates)) + geom_step(direction='hv') + theme_bw() + scale_y_log10()

  # compute/plot simple distribution info
  obs_counts_simple <- obs_counts %>%
    group_by(obs_date_bin) %>%
    summarize(n=sum(n_sites_at), .groups='drop')

  # merge county polygons into state polygons
  conus_states <- group_by(us_counties_sf, state) %>% summarise() %>% st_geometry() %>% st_transform(crs = plot_crs)

  # compute the bounding box
  box_sf <- tibble(
    lat = sf::st_bbox(all_lakes)[c('ymin','ymin','ymax','ymax')],
    lon = sf::st_bbox(all_lakes)[c('xmin','xmax','xmax','xmin')]) %>%
    sf::st_as_sf(coords=c('lon','lat'), crs=sf::st_crs(all_lakes), remove=F)

  # get the bin labels
  bin_levels <- rev(levels(all_lakes$obs_date_bin))

  # choose the colors
  # palette <- c('#f4b255','#41bf9b','#3ca36f','#6a6384','#c4b8ca') %>% # https://colorswall.com/palette/371/
  # palette <- c('#cb3c97','#c97399','#eaeed2','#76e5bd','#2dd698') %>% # https://colorswall.com/palette/967
  # palette <- c('#cb3c97','#703ccb','#3c97cb','#3ccb70','#97cb3c') %>%
  palette <- c('#cb3c97','#3c97cb','#3ccb70') %>%
    setNames(bin_levels)
  all_lakes_styled <- all_lakes %>%
    mutate(color = palette[as.character(all_lakes$obs_date_bin)])

  # create and save one plot for each additional obs date bin
  for(bin in c(NA, bin_levels)) {
    message(sprintf('plotting for %s obs dates', bin))
    png(filename = gsub('.png', sprintf('_%s.png', bin), fileout_base, fixed=TRUE),
        width = 7, height = 4.75, units = 'in', res = 350)
    par(omi = c(0,0,0,0), mai = c(0,0,0,0), xaxs = 'i', yaxs = 'i')

    # set the viewbox
    plot(st_geometry(box_sf), col = NA, border = NA, reset = FALSE)

    # plot the background state borders
    plot(conus_states, col = NA, border = 'lightgray', lwd = 1.5, add = TRUE)

    # pick out just the not-huge lakes having this many or more observations
    size_threshold <- 1.3e9

    # plot the great lakes in gray
    big_lakes <- all_lakes_styled %>%
      filter(area >= size_threshold)
    plot(st_geometry(big_lakes), col='lightgray', border=NA, add=TRUE)

    # plot the lakes in layers from least-observed from most-observed
    if(!is.na(bin)) {
      plot_bins <- bin_levels[seq_len(match(bin, bin_levels))]
      for(plot_bin in rev(plot_bins)) {
        all_lakes_subset <- all_lakes_styled %>%
          filter(area < size_threshold, obs_date_bin == plot_bin)

        # plot the lakes using the designated color (fill and border)
        plot(
          st_geometry(all_lakes_subset),
          col = all_lakes_subset$color,
          border = all_lakes_subset$color, lwd = 0.2,
          add = TRUE)

        # plot a light circle around each lake centroid to give the tiny lakes some visibility
        plot(
          st_centroid(st_geometry(all_lakes_subset)),
          col = gsub('ff', 'cc', all_lakes_subset$color),
          lwd = 0.5,
          add = TRUE, cex = 0.3)
      }
    }

    # add legend
    if(!is.na(bin)) {
      labels <- obs_counts_simple %>% filter(obs_date_bin %in% plot_bins) %>%
        mutate(label = sprintf('%s obs dates (%d lakes)', obs_date_bin, n)) %>%
        pull(label) %>%
        rev()
      legend(-200000, -600000,
             legend = labels,
             fill = palette[1:length(labels)], border = NA,
             box.col = NA,
             horiz = FALSE)
    }

    dev.off()
  }
}

plot_lake_obs_map(
  fileout_base = '8_viz/out/lake_obs_map.png',
  temp_obs_ind = '7b_temp_merge/out/temp_data_with_sources.feather.ind',
  lakes_sf_fl = "1_crosswalk_fetch/out/canonical_lakes_sf.rds",
  us_counties_sf = us_counties_sf,
  plot_crs = I("+init=epsg:2811"))
