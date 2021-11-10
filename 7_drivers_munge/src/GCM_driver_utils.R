library(sf)
library(ggplot2)

map_centroids <- function(centroids_sf_rds, out_file) {
  centroids_sf <- readRDS(centroids_sf_rds)
  centroid_plot <- ggplot() +
    geom_sf(data=centroids_sf, color='dodgerblue', size=0.5)

  ggsave(out_file, centroid_plot, width=10, height=8, dpi=300)

  return(out_file)
}
