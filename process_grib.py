# process live feeds
import pygrib
import pandas as pd
from utils import *
from numpy import arctan2, pi, where
from datetime import datetime, timedelta
from utils import regex_local, regex_remote
import re
from pathlib import Path

def process_grib(filename, df_unique_grid):
    """process one grib2 file given the file name
    intersect with effective grid_id
    engineer the features.
    This file is for extracting winter related data (snowfall, freezing rain etc) besides normal weather data"""
    if filename.exists():
        grbs = pygrib.open(str(filename))
    analysis_date_str, analysis_hour_str, valid_hour_str = re.findall(regex_local, str(filename))[0]
    analysis_dttm = datetime.strptime(f"{analysis_date_str} {analysis_hour_str}:00:00", "%Y%m%d %H:%M:%S")
    analysis_dttm_str = analysis_dttm.strftime(dttm_format)
    analysis_dttm_prev_h = analysis_dttm - timedelta(hours=1)
    
    valid_hour = int(valid_hour_str)
    valid_dttm = analysis_dttm + timedelta(hours=valid_hour)
    valid_dttm_str = valid_dttm.strftime(dttm_format)

    version_hrrr = get_hrrr_ver(valid_hour_str, grbs.messages)

    # temperature at 2 m above ground
    index_temperature_2m = dict_var_index["temperature_2m"][version_hrrr]
    grb_temperature_2m = grbs[index_temperature_2m]
    
    # u and v component of wind at 10 m above ground
    index_wind_10m_u = dict_var_index["wind_10m_u"][version_hrrr]
    index_wind_10m_v = dict_var_index["wind_10m_v"][version_hrrr]
    index_wind_10m = dict_var_index["wind_10m"][version_hrrr]
    grb_wind_10m_u = grbs[index_wind_10m_u]
    grb_wind_10m_v = grbs[index_wind_10m_v]
    grb_wind_10m = grbs[index_wind_10m]

    # composite reflectivity
    index_composite_reflectivity = dict_var_index["composite_reflectivity"][version_hrrr]
    grb_composite_reflectivity = grbs[index_composite_reflectivity]

    # wind gust
    index_wind_gust = dict_var_index["wind_gust"][version_hrrr]
    grb_wind_gust = grbs[index_wind_gust]

    # CAPE 255
    index_CAPE255 = dict_var_index["CAPE255"][version_hrrr]
    grb_CAPE255 = grbs[index_CAPE255]

    # initilize indices
    index_precipitation = 0 # for forecast hours 0 and 1
    index_precipitation_past_h = 0 # for forecast hours other than 0 and 1
    index_snowfall = 0
    index_freezerain = 0
    index_helicity = 0

    if valid_hour == 0:
        # use precipitation, snowfall, freezing rain and helicity
        # from previous analysis hour and forecast 1 hour as data 
        # for valid hour 0
        filename_a_prev_h_f1 = timestamp_to_filename(analysis_dttm_prev_h, "01", filename.parent)
        if filename_a_prev_h_f1.exists():
            grbs_a_prev_h_f1 = pygrib.open(str(filename_a_prev_h_f1))
        
        version_hrrr_a_prev_h_f1 = get_hrrr_ver("01", grbs_a_prev_h_f1.messages)
        index_precipitation = dict_var_index["precipitation_tot"][version_hrrr_a_prev_h_f1]
        index_snowfall = dict_var_index["snowfall_tot"][version_hrrr_a_prev_h_f1]
        index_freezerain = dict_var_index["freezing_rain"][version_hrrr_a_prev_h_f1]
        index_helicity = dict_var_index["helicity"][version_hrrr_a_prev_h_f1]

        values_precipitation = grbs_a_prev_h_f1[index_precipitation].values
        values_snowfall = grbs_a_prev_h_f1[index_snowfall].values
        values_freezerain = grbs_a_prev_h_f1[index_freezerain].values
        values_helicity = grbs_a_prev_h_f1[index_helicity].values
    elif valid_hour == 1:
        # valid hour 1 has data for precipitation snowfall and freezing rain for hour 0-1
        index_precipitation = dict_var_index["precipitation_tot"][version_hrrr]
        index_snowfall = dict_var_index["snowfall_tot"][version_hrrr]
        index_freezerain = dict_var_index["freezing_rain"][version_hrrr]
        index_helicity = dict_var_index["helicity"][version_hrrr]

        values_precipitation = grbs[index_precipitation].values
        values_snowfall = grbs[index_snowfall].values
        values_freezerain = grbs[index_freezerain].values
        values_helicity = grbs[index_helicity].values
    elif valid_hour > 1:
        # for hour 2 and forward, precipitation and helicity has the data for (valid_hour-1) to valid_hour.
        # but for snowfall and freezing rain, it has data for hour 0 to valid_hour, so the past hour data is
        # calculated as the difference: (0 to valid_hour) - (0 to (valid_hour-1))
        valid_prev_h = int(valid_hour_str) - 1
        valid_prev_h_str = f"{valid_prev_h:02}"

        index_precipitation_past_h = dict_var_index["precipitation_tot_past_h"][version_hrrr]
        index_snowfall = dict_var_index["snowfall_tot"][version_hrrr]
        index_freezerain = dict_var_index["freezing_rain"][version_hrrr]
        index_helicity = dict_var_index["helicity"][version_hrrr]

        # for snowfall and freezing rain
        # for forecast hours 2 and later, need to load the GRIB2 for valid_prev_h of the same analysis hour
        filename_prev_h = Path(str(filename).replace(f"wrfsfcf{valid_hour_str}", f"wrfsfcf{valid_prev_h_str}"))
        if filename_prev_h.exists():
            grbs_prev_h = pygrib.open(str(filename_prev_h))

        values_precipitation = grbs[index_precipitation_past_h].values
        values_helicity = grbs[index_helicity].values
        # the values of snowfall and freezerain could be negative, replace the negative values with zero
        values_snowfall = grbs[index_snowfall].values - grbs_prev_h[index_snowfall].values
        values_snowfall = where(values_snowfall<0, 0.0, values_snowfall)
        values_freezerain = grbs[index_freezerain].values - grbs_prev_h[index_freezerain].values
        values_freezerain = where(values_freezerain<0, 0.0, values_freezerain)
    
    lst_unique_grid = df_unique_grid["hrrr_id"].tolist()
    df = pd.DataFrame({"timestamp_analysis": analysis_dttm_str,
                       "timestamp_valid": valid_dttm_str,
                       "temperature": grb_temperature_2m.values.flatten()[lst_unique_grid],
                       "wind_10m_u": grb_wind_10m_u.values.flatten()[lst_unique_grid],
                       "wind_10m_v": grb_wind_10m_v.values.flatten()[lst_unique_grid],
                       "wind_10m": grb_wind_10m.values.flatten()[lst_unique_grid],
                       "precipitation_tot": values_precipitation.flatten()[lst_unique_grid],
                       "snowfall_tot": values_snowfall.flatten()[lst_unique_grid],
                       "freezing_rain": values_freezerain.flatten()[lst_unique_grid],
                       "composite_reflectivity": grb_composite_reflectivity.values.flatten()[lst_unique_grid],
                       "wind_gust": grb_wind_gust.values.flatten()[lst_unique_grid],
                       "CAPE255": grb_CAPE255.values.flatten()[lst_unique_grid],
                       "helicity": values_helicity.flatten()[lst_unique_grid],
                       "hrrr_id": lst_unique_grid
                       })

    # map hrrr_id to grid_id
    df = pd.merge(df, df_unique_grid, on="hrrr_id").drop("hrrr_id", axis=1)

    # engineer features
    # convert str to UTC datetime and local datetime
    df = df.assign(timestamp_analysis_local=df.timestamp_analysis.apply(str_local_timestamp))
    df = df.assign(timestamp_valid_local=df.timestamp_valid.apply(str_local_timestamp))

    # convert temperature unit
    df = df.assign(temperature=df.temperature.apply(Kelvin_to_Fahrenheit))

    # convert wind speed unit
    df = df.assign(wind_10m=df.wind_10m * unit_mps_mph)
    df = df.assign(wind_gust=df.wind_gust * unit_mps_mph)

    # wind angle
    df["wind_angle"] = df.apply(lambda x: arctan2(x["wind_10m_v"], x["wind_10m_u"])*180/pi, axis=1)
    df = df.assign(wind_angle=df.wind_angle.apply(angle360))
    df = df.assign(wind_direction=df.wind_angle.apply(angle_desc))

    df = df.drop(["wind_10m_u", "wind_10m_v", "wind_angle"], axis=1)

    df = df[["timestamp_analysis", "timestamp_valid", "timestamp_analysis_local", "timestamp_valid_local", \
    "grid_id", "lat", "lon", \
    "temperature", "wind_10m", "wind_direction", "precipitation_tot", \
    "snowfall_tot", "freezing_rain", "composite_reflectivity", "wind_gust", \
    "CAPE255", "helicity"]]
    df = df.rename(columns={"timestamp_analysis":"timestamp_analysis_utc", "timestamp_valid":"timestamp_valid_utc"})

    return df

if __name__ == "__main__":
    filename = Path("hrrr.20201211.t00z.wrfsfcf00.grib2")
    df_unique_grid = pd.read_csv(f"{storm_dir}/data_GIS/unique_grid_id.csv")
    df = process_grib_live(filename, df_unique_grid)
    