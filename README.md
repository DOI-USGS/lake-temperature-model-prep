# lake-temperature-model-prep

All key outputs from this pipeline are on [google drive](https://drive.google.com/drive/u/1/folders/1BKPxm1-eH6xT_Q_zx7w1iyEDVgeTYjd_?ogsrc=32).

The following files are dependencies in pipeline #2 lake-temperature-process-models:
  -  [`feature_crosswalk.rds`](https://drive.google.com/drive/u/1/folders/1R4FBzDUCRMpK-rwqMt_NTujq68aYojTQ) and [`.ind`](https://github.com/USGS-R/lake-temperature-process-models/tree/master/1_get_lake_attr/in)
  -  [`lakes_sf.rds`](https://drive.google.com/drive/u/1/folders/1R4FBzDUCRMpK-rwqMt_NTujq68aYojTQ) and [`.ind`](https://github.com/USGS-R/lake-temperature-process-models/tree/master/1_get_lake_attr/in)
  -  NLDAS_grid coords and cell resolution  
  
The following files are dependencies in pipeline #3 lake-temperature-neural-networks:
  -  [`merged_temp_data_daily.feather`](https://drive.google.com/drive/u/1/folders/1pbhIjfYUPZ4lEICm5zwJFjIjGYEz1qwi) and [`.ind`](https://github.com/USGS-R/lake-temperature-neural-network/in/merged_temp_data_daily.feather.ind)
   -  [`feature_crosswalk.rds`](https://drive.google.com/drive/u/1/folders/1pbhIjfYUPZ4lEICm5zwJFjIjGYEz1qwi) and [`.ind`](https://github.com/USGS-R/lake-temperature-neural-network/in/feature_crosswalk.rds.ind)
  
If new cooperator data is added, and you'd like to trigger a rebuild:
  1. `scmake('coop_all_files', force = TRUE)` and then `scmake('6_temp_coop_fetch')`. This should download the new files that have yet to be parsed.
  2. `scmake('7a_temp_coop_munge/log/7a_temp_coop_munge_tasks.ind', force = TRUE)`. This will fail if a cooperator data file does not have a parser - and will tell you which files need parsers. Add parsers to `7a_temp_coop_munge/src/data_parser`. Parser functions must match file name, or must define rules to match files to parsers in `find_parsers` function [here](https://github.com/USGS-R/lake-temperature-model-prep/blob/master/7a_temp_coop_munge/src/parsing_task_fxns.R). This is useful if several files can use the same parser. If you add new scripts that contain parsers (e.g., don't add new functions to current scripts), then be sure to add the new files to the [makefile sources](https://github.com/USGS-R/lake-temperature-model-prep/blob/master/7a_temp_coop_munge/src/parsing_task_fxns.R#L89)
  3. Once `scmake('7a_temp_coop_munge/log/7a_temp_coop_munge_tasks.ind')` runs successfully, final step is to run `scmake('7b_temp_merge')`

If any of these files are changed / updated in this pipeline, remember to: 
  1. copy the update .rds file to the dependent pipeline's drive (which is hyperlinked above) and to _ALSO_
  2. copy the updated .ind file to the dependent pipeline's github repository (which is also hyperlinked above)
  
## Tallgrass

Some targets need to be prepared here to support execution of `mntoha-data-release`:
```
1_crosswalk_fetch/out/canonical_lakes_sf.rds
7b_temp_merge/out/temp_data_with_sources.feather
```

To build these targets, we need certain R packages. Here's a recipe for creating a sufficient conda environment:
```sh
conda create -n lakes_prep
source activate lakes_prep
conda install -c conda-forge r-raster r-readxl r-doMC r-leaflet r-sys r-e1071 r-class r-KernSmooth r-askpass r-classInt r-DBI r-fs r-openssl r-sf r-units r-curl r-gargle r-httr r-purrr r-uuid r-devtools r-dplyr r-tidyselect r-BH r-plogr r-optparse r-storr r-getopt r-readr r-tidyr r-feather r-lwgeom r-maps r-ncdf4 r-lubridate r-generics
R
install.packages(c('smoothr','googledrive'))
devtools::install_github('USGS-R/lakeattributes')
devtools::install_github('richfitz/remake')
devtools::install_github('USGS-R/scipiper')
install.packages(c('dataRetrieval', 'sbtools'))
```

In subsequent sessions, we can get going with:
```sh
ssh tallgrass.cr.usgs.gov
cd /caldera/projects/usgs/water/iidd/datasci/lake-temp/lake-temperature-model-prep
source activate lakes_prep
```

...but after all that, I'm stuck now on a need for Drive authentication and am just going
to build locally and push back up to this repo.
```r
gd_get('1_crosswalk_fetch/out/canonical_lakes_sf.rds.ind')
gd_get('7b_temp_merge/out/temp_data_with_sources.feather.ind')
```
```sh
scp 1_crosswalk_fetch/out/canonical_lakes_sf.rds tallgrass.cr.usgs.gov:/caldera/projects/usgs/water/iidd/datasci/lake-temp/lake-temperature-model-prep/1_crosswalk_fetch/out/
scp 7b_temp_merge/out/temp_data_with_sources.feather tallgrass.cr.usgs.gov:/caldera/projects/usgs/water/iidd/datasci/lake-temp/lake-temperature-model-prep/7b_temp_merge/out/
```
