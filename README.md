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

### Adding new spatial data that uses a non-NHDHR identifier and generating a new crosswalk
When data (such as temperature, depth, clarity, etc) are referenced to a non-NHDHR identifier, we need to generate a new crosswalk file that shows the code how to link the `site_id` (NHD high-res ID) to the other identifier (often a state agency ID). The two common ways this crosswalk is generated is by using an approximate polygon overlap to consider two lakes "the same" or using a lat/lon sampling point from a lake with a point-in-polygon analysis. 

#### Crosswalk: Alternative ID comes from polygon spatial data
Often a state agency has a shapefile (or other) that has polygons for each lake they study, as well as attributes that include the agency ID, which we'll want to crosswalk. Commonly, this shapefile doesn't come from an easy-to-code web service, as it is either emailed to us, or downloaded from a UI that doesn't make it easy to code against. (If, however, there is a service to code against, you can skip steps 1 and 2 and go right to step 3, but write your function to access the remote data and write to `sf` as .rds instead of opening a local file).

1) First, we upload the zip file that contains the raw/original data manually onto google drive to [`1_crosswalk_fetch/in`](https://drive.google.com/drive/u/0/folders/1T8cpB2YUXbP_q4fFNCd3elOG6_FU1TvU) and then in the console, create an indicator file for that remote dataset using `gd_confirm_posted("1_crosswalk_fetch/in/{my_filename}.zip.ind")`. This only needs to be done once, and now the .ind file and the corresponding build file will appear locally and must now be checked into version control so other collaborators will have access to this information. 

2) Next, we add the `data_file` (`1_crosswalk_fetch/in/{my_filename}.zip`) to the `getters.yml`, using the format
```yaml
  1_crosswalk_fetch/in/{my_filename}.zip:
    command: gd_get('1_crosswalk_fetch/in/{my_filename}.zip.ind')
```
This will allow you or others to access this dataset to help build other targets that use it. 

3) Next, convert this file into an `sf` object and save it as an .rds file in the `1_crosswalk_fetch.yml`, using something along the lines of 
```yaml
  1_crosswalk_fetch/out/{stateXY_lakes}_sf.rds.ind:
    command: stateXY_zip_to_sf(
      out_ind = target_name,
      layer = I("stateXY_Lakes"),
      zip_ind = '1_crosswalk_fetch/in/{my_filename}.zip.ind')
```
You may need to create a custom function to do this, since each of the files seem to be different enough. This function should write a single file, which will be a MULTIPOLYGON `sf` simple feature collection with only one field (other than the spatial information in `geometry`), which will be `site_id`. `site_id` will be the data source alternative (non-NHDHR) identifier as a character. 


4) Next, add the `data_file` to the `getters.yml`, using the format
```yaml
  1_crosswalk_fetch/out/{stateXY_lakes}_sf.rds:
    command: gd_get('1_crosswalk_fetch/out/{stateXY_lakes}_sf.rds.ind')
```

The next set of steps will cover the creation of the ID crosswalk for this dataset, `2_crosswalk_munge/out/{stateXY}_nhdhr_xwalk.rds.ind`:

5) Now, take advantage of a couple generic functions that use the standardized outputs of steps 3 & 4 to evaluate polygon overlap. The `crosswalk_poly_intersect_poly()` function is the first of these functions, and has inputs for the outgoing indicator file, the two polygons needed (one for your specific new dataset, and the other as the canonical source of NHDHR lakes), and the CRS that you'd like to perform the overlap analysis in. The `poly1_ID_name` argument is special - this is where you will define the site identifier naming convention for this crosswalk. This is the field/column name that will show up in the `{stateXY}_nhdhr_xwalk.rds` alongside the `site_id` for the NHDHR ID. I've been using uppercase letters for this argument, and it is going to be filled with the values of the alternative (non-NHDHR) identifier found in the `site_id` output of step #3. Here is what that new target recipe would look like: 
```yaml
  2_crosswalk_munge/out/{stateXY}_nhdhr_intersect.rds.ind:
    command: crosswalk_poly_intersect_poly(
      target_name,
      poly1_ind_file = '1_crosswalk_fetch/out/{stateXY_lakes}_sf.rds.ind',
      poly2_ind_file = '1_crosswalk_fetch/out/canonical_lakes_sf.rds.ind',
      poly1_ID_name = I("{stateXY}_ID"),
      crs=I(26915))
```
6) use the second generic polygon overlap function to choose a single polygon from intersected polygons from the file you just created a target for:
```yaml
  2_crosswalk_munge/out/mglp_nhdhr_xwalk.rds.ind:
    command: choose1_poly_intersect_poly(
      target_name,
      intersect_ind_file = '2_crosswalk_munge/out/{stateXY}_nhdhr_intersect.rds.ind',
      poly1_ID_name = I("{stateXY}_ID"))
```
This is broken up into two functions because the expensive analysis happens in `crosswalk_poly_intersect_poly()` and if we ever want to change the selection criteria in `choose1_poly_intersect_poly()`, doing so would be pretty quick. When we wrote these functions I think we thought it would be likely we'd revisit this matching criteria in the future. 


7) As before, we're using a shared cache, so add both targets to getters.yml:
```yaml
  2_crosswalk_munge/out/{stateXY}_nhdhr_intersect.rds:
    command: gd_get('2_crosswalk_munge/out/{stateXY}_nhdhr_intersect.rds.ind')    
  2_crosswalk_munge/out/{stateXY}_nhdhr_xwalk.rds:
    command: gd_get('2_crosswalk_munge/out/{stateXY}_nhdhr_xwalk.rds.ind')
```

8) Add the new indicator file target from #6 to the depends of the `2_crosswalk_munge` target:
```yaml
targets:
  2_crosswalk_munge:
    depends:
      ...
      - 2_crosswalk_munge/out/{stateXY}_nhdhr_xwalk.rds.ind
```

9) Lastly, build `scmake('2_crosswalk_munge')` and check the new built files (and code files) into version control and create a PR. You will now be able to use the crosswalk file to link data from this set of identifiers into the modeling system. The original file you added in step #1 `1_crosswalk_fetch/in/` should already be gitignored, but make sure this doesn't show up in your commits because we don't want to check large files (or really any data files) into github. 


#### Crosswalk: Alternative ID comes from point-based spatial data (such as monitoring locations)
Point-based data that contain non-NHDHR identifiers is a bit simpler to set up. Similar to vector polygon data, some datasets need to be manually uploaded to drive and indicated prior to being able to process them (steps 1 and 2 from above) and other point-based data can be directly accessed via services, such as the water quality portal monitoring locations. 

Skipping ahead to step #3 above (assuming you've building an `sf` object and saving it as an .rds), create a POINT simple feature collection with the required field `site_id`, other optional fields (e.g., "OrganizationIdentifier", and "resultCount" are in the `1_crosswalk_fetch/out/wqp_lake_secchi_sites_sf.rds` file for diagnostic purposes), with the required `geometry` as well. Creating this .rds file (assumed here as `1_crosswalk_fetch/out/{pointXY}_sf.rds`) and indicator file likely requires a custom function, see `fetch_navico_points()` for a simple example. 

1) After this file and the .ind in `getters.yml is created, you can move on to the generic function `crosswalk_points_in_poly()` to create the crosswalk file:
```yaml
  2_crosswalk_munge/out/{pointXY}_nhdhr_xwalk.rds.ind:
    command: crosswalk_points_in_poly(target_name,
      poly_ind_file = '1_crosswalk_fetch/out/canonical_lakes_sf.rds.ind',
      points_ind_file = '1_crosswalk_fetch/out/{pointXY}_sf.rds.ind',
      points_ID_name = I("{POINTXY}_ID"))
```
As with above, the `points_ID_name` is what you want to refer to the crosswalk field in future use, this is not a field that matches an existing prior name. 

2) Add the indicator file(s) to getters.yml
```yaml
  1_crosswalk_fetch/out/{pointXY}_sf.rds:
    command: gd_get('1_crosswalk_fetch/out/{pointXY}_sf.rds.ind')    
  2_crosswalk_munge/out/{pointXY}_nhdhr_xwalk.rds:
    command: gd_get('2_crosswalk_munge/out/{pointXY}_nhdhr_xwalk.rds.ind')
```

3) Add the new indicator file target from #6 to the depends of the `2_crosswalk_munge` target:
```yaml
targets:
  2_crosswalk_munge:
    depends:
      ...
      - 2_crosswalk_munge/out/{pointXY}_nhdhr_xwalk.rds.ind
```

8) Lastly, build `scmake('2_crosswalk_munge')` and check the new built files (and code files) into version control and create a PR. You will now be able to use the crosswalk file to link data from this set of identifiers into the modeling system.


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
