# Extract and transform weather variables from raw HRRR, convert them to tabular data
### Most recently 24 hours of forecast data at t00z, t06z, t12z and t18z;
### Use Google gsutil to download multiple grib2 files from HRRR repository in parallel;
### Extract or calculate values for precipitation, snowfall, freezing rain and helicity depending on the model valid hour;
### Built-in mechanism to deal with different versions of HRRR;
### Conversion of UTC to local time;