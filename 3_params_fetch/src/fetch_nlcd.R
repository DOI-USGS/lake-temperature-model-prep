
fetch_nlcd_classes <- function(out_ind){
  nlcd_classes <- data.frame(class = c(0, 11,12,21,22,23,24,31,41,42,43,51,52,71,72,73,74,81,82,90,95),
                             type=c('No Data', 'Open Water','Perennial Ice/Snow','Developed,
                                    Open Space','Developed, Low Intensity', 'Developed, Medium Intensity',
                                    'Developed, High Intensity','Barren Land (Rock/Sand/Clay)',
                                    'Deciduous Forest','Evergreen Forest','Mixed Forest','Dwarf Scrub',
                                    'Scrub/Shrub','Grassland/Herbaceous', 'Sedge/Herbaceuous','Lichens',
                                    'Moss','Pasture/Hay','Cultivated Crops','Woody Wetlands',
                                    'Emergent Herbaceous Wetlands'))
  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(nlcd_classes, data_file)
  gd_put(out_ind, data_file)
}

fetch_nlcd_classes_canopyheight <- function(out_ind) {
  # fetch from Winslow et al
  nlcd_heights <- read.csv('https://media.nature.com/original/nature-assets/sdata/2017/sdata201753/extref/sdata201753-s2.csv') %>%
    rename(type = `Landcover.Name`, height_m = `Height.Value..meters.`, class = `NLCD.ID`)

  data_file <- scipiper::as_data_file(out_ind)
  saveRDS(nlcd_heights, data_file)
  gd_put(out_ind, data_file)
}
