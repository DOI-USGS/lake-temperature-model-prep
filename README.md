# lake-temperature-model-prep

All key outputs from this pipeline are on [google drive](https://drive.google.com/drive/u/1/folders/1BKPxm1-eH6xT_Q_zx7w1iyEDVgeTYjd_?ogsrc=32).

The following files are dependencies in pipeline #2 lake-temperature-process-models:
  -  [`feature_crosswalk.rds`](https://drive.google.com/drive/u/1/folders/1R4FBzDUCRMpK-rwqMt_NTujq68aYojTQ) and [`.ind`](https://github.com/USGS-R/lake-temperature-process-models/tree/master/1_get_lake_attr/in)
  -  [`lakes_sf.rds`](https://drive.google.com/drive/u/1/folders/1R4FBzDUCRMpK-rwqMt_NTujq68aYojTQ) and [`.ind`](https://github.com/USGS-R/lake-temperature-process-models/tree/master/1_get_lake_attr/in)
  -  NLDAS_grid coords and cell resolution  
  
The following files are dependencies in pipeline #3 lake-temperature-neural-networks:
  -  [`merged_temp_data_daily.feather`](https://drive.google.com/drive/u/1/folders/1pbhIjfYUPZ4lEICm5zwJFjIjGYEz1qwi) and [`.ind`](https://github.com/USGS-R/lake-temperature-neural-network/in/merged_temp_data_daily.feather.ind)
   -  [`feature_crosswalk.rds`](https://drive.google.com/drive/u/1/folders/1pbhIjfYUPZ4lEICm5zwJFjIjGYEz1qwi) and [`.ind`](https://github.com/USGS-R/lake-temperature-neural-network/in/feature_crosswalk.rds.ind)
  
If any of these files are changed / updated in this pipeline, remember to: 
  1. copy the update .rds file to the dependent pipeline's drive (which is hyperlinked above) and to _ALSO_
  2. copy the updated .ind file to the dependent pipeline's github repository (which is also hyperlinked above)
