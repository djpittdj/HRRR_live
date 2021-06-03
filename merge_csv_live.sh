#!/bin/bash

work_dir=""
echo "timestamp_valid_local,grid_id,lat,lon,temperature,wind_10m,wind_direction,precipitation_tot,snowfall_tot,freezing_rain,composite_reflectivity,wind_gust,CAPE255,helicity" > ${work_dir}/data_out/hrrr_territory_live.csv
grep -vh "timestamp" ${work_dir}/CSV/*csv >> ${work_dir}/data_out/hrrr_territory_live.csv
