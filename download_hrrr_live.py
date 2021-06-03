# download most recent 48 hours of data
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import re
import subprocess
import os
from utils import regex_local, regex_remote, storm_dir, get_lst_diff, filter_hours

gsutil_exe=""
gcp_hrrr_loc="gs://high-resolution-rapid-refresh"
work_dir = storm_dir/"HRRR_live"
grib2_dir = work_dir/"GRIB2"
if not grib2_dir.exists():
    grib2_dir.mkdir()

def analysis_hour_to_grib2(analysis_hour, mode="remote"):
    """map analysis hour to remote or local file names"""
    lst = []
    # 1 hour before analysis_hour and forecast 1 hour
    analysis_hour_prev = analysis_hour - timedelta(hours=1)
    date_str = analysis_hour_prev.strftime("%Y%m%d")
    hour_str = analysis_hour_prev.strftime("%2H")
    if mode=="remote":
        lst.append(f"{gcp_hrrr_loc}/hrrr.{date_str}/conus/hrrr.t{hour_str}z.wrfsfcf01.grib2")
    elif mode=="local":
        lst.append(f"{grib2_dir}/hrrr.{date_str}.t{hour_str}z.wrfsfcf01.grib2")
    else:
        print("mode not available")

    date_str = analysis_hour.strftime("%Y%m%d")
    hour_str = analysis_hour.strftime("%2H")
    for h in range(49):
        h_str = f"{h:02}"
        if mode=="remote":
            lst.append(f"{gcp_hrrr_loc}/hrrr.{date_str}/conus/hrrr.t{hour_str}z.wrfsfcf{h_str}.grib2")
        elif mode=="local":
            lst.append(f"{grib2_dir}/hrrr.{date_str}.t{hour_str}z.wrfsfcf{h_str}.grib2")
        else:
            print("mode not available")
    return lst

def convert_mode(name):
    """convert from local to remote or reverse"""
    if name.startswith(f"{work_dir}"):
        # from local to remote
        matched = re.findall(regex_local, name)[0]
        date_str, analysis_hour, forecast_hour = matched
        result = f"{gcp_hrrr_loc}/hrrr.{date_str}/conus/hrrr.t{analysis_hour}z.wrfsfcf{forecast_hour}.grib2"
    elif name.startswith("gs:"):
        # from remote to local
        matched = re.findall(regex_remote, name)[0]
        date_str, analysis_hour, forecast_hour = matched
        result = f"{grib2_dir}/hrrr.{date_str}.t{analysis_hour}z.wrfsfcf{forecast_hour}.grib2"
    return result

def get_local_day(filename):
    return re.findall(regex_local, filename)[0][0]

def gsutil_result_to_list(result_in):
    """process result from gsutil"""
    result_out = result_in.stdout.decode("utf-8").split('\n')
    # there could a blank line
    result_out = list(filter(lambda x: "gs:" in x, result_out))
    result_out.sort()
    return result_out

def extract_date_hour(x):
    date_str, analysis_hour_str, valid_hour_str = re.findall(regex_remote, x)[0]
    return f"{date_str}-{analysis_hour_str}"

def get_available_models():
    """get the list of model available"""
    current_dttm = datetime.today()
    current_day = current_dttm.date()
    current_day_str = current_day.strftime("%Y%m%d")
    next_day = current_day + timedelta(days=1)
    next_day_str = next_day.strftime("%Y%m%d")

    # query today and tomorrow's files
    result = subprocess.run([gsutil_exe, 
                            "ls", 
                            f"{gcp_hrrr_loc}/hrrr.{current_day_str}/conus/hrrr.*wrfsfc*grib2", 
                            f"{gcp_hrrr_loc}/hrrr.{next_day_str}/conus/hrrr.*wrfsfc*grib2"], 
                            stdout=subprocess.PIPE)
    result = gsutil_result_to_list(result)
    # a list of grib files that are relevant with analysis hour in 00, 06, 12 or 18
    result48 = list(filter(filter_hours, result))

    return result48

def get_analysis_hours(latest_date_str, latest_analysis_hour_str):
    """get the latest 48 analysis hours"""
    # change format
    latest_date_str = datetime.strptime(latest_date_str, "%Y%m%d").strftime("%Y-%m-%d")
    latest_analysis_hour = int(latest_analysis_hour_str)
    analysis_hours = []
    if latest_analysis_hour>=0 and latest_analysis_hour<6:
        # t00z is the latest
        end_dttm_str = f"{latest_date_str} 00:00:00"
        end_hour_str = "00"
    elif latest_analysis_hour>=6 and latest_analysis_hour<12:
        # t06z is the latest
        end_dttm_str = f"{latest_date_str} 06:00:00"
        end_hour_str = "06"
    elif latest_analysis_hour>=12 and latest_analysis_hour<18:
        # t12z is the latest
        end_dttm_str = f"{latest_date_str} 12:00:00"
        end_hour_str = "12"
    else:
        # t18z is the latest
        end_dttm_str = f"{latest_date_str} 18:00:00"
        end_hour_str = "18"

    analysis_hours = pd.date_range(end=end_dttm_str, freq="6h", periods=4)

    return analysis_hours

def download_hrrr_live():
    """download new raw files and remove unnecessary ones
    return lists of new and old files"""
    # actual local list
    lst_local_grib_actual = list(grib2_dir.glob("*grib2"))
    lst_local_grib_actual = list(map(str, lst_local_grib_actual))
    lst_local_grib_actual.sort()

    result48 = get_available_models()
    # unique date_hour
    lst_date_hour = list(set(map(extract_date_hour, result48)))
    lst_date_hour.sort()
    latest_date_hour = lst_date_hour[-1]
    num_files_latest_date_hour = len(list(filter(lambda x: extract_date_hour(x)==latest_date_hour, result48)))
    # a dict that has date_hour and number of files available for that date_hour
    dict_day_hour = {}
    for i in lst_date_hour:
        dict_day_hour[i] = len(list(filter(lambda x: extract_date_hour(x)==i, result48)))
    # a list of (date_hour, n. files) tuple with n. files equal to 49
    lst = list(filter(lambda x: x[1]==49, dict_day_hour.items()))
    # only keep the date_hour
    lst = [i[0] for i in lst]
    lst.sort()

    # if the latest model has 49 files, meaning it's complete
    if num_files_latest_date_hour == 49:
        latest_date_str, latest_analysis_hour_str = latest_date_hour.split('-')
    # use the previous model which has 49 files
    elif len(lst) != 0:
        latest_date_str, latest_analysis_hour_str = lst[-1].split('-')
    # if there's no avaiable latest model
    else:
        latest_date_str, latest_analysis_hour_str, _ = re.findall(regex_local, lst_local_grib_actual[-1])[0]

    # initialize the lists
    lst_local_grib_expected, lst_local_grib_added, lst_local_grib_remove = [], [], []
    analysis_hours = get_analysis_hours(latest_date_str, latest_analysis_hour_str)
    for h in analysis_hours:
        lst_local_grib_expected.extend(analysis_hour_to_grib2(h, "local"))

    # files to be downloaded
    lst_local_grib_added = get_lst_diff(lst_local_grib_expected, lst_local_grib_actual)
    pd.DataFrame(lst_local_grib_added, columns=["filename"]).to_csv(f"{str(work_dir)}/lst_local_grib_added.csv", index=False)

    # if the expected and the actual are not the same
    if lst_local_grib_actual != lst_local_grib_expected:
        # unique days of the lst_local_grib_added
        unique_days = list(set(map(get_local_day, lst_local_grib_added)))
        unique_days.sort()

        # for each day, download files in bulk and rename them
        for day in unique_days:
            lst = list(filter(lambda x: get_local_day(x)==day, lst_local_grib_added)) # a list of filenames for that day
            lst = list(map(convert_mode, lst)) # convert local list of filenames to remote filenames
            s = ''
            for i in lst:
                s = s + i + ' '
            # download files from GCP
            os.system(f"{gsutil_exe} ls {s} | {gsutil_exe} -m cp -I {str(grib2_dir)}")
            # rename files
            os.system(f"rename {str(grib2_dir)}/hrrr.t {str(grib2_dir)}/hrrr.{day}.t {str(grib2_dir)}/*grib2")
        
    # files to be deleted
    lst_local_grib_remove = get_lst_diff(lst_local_grib_actual, lst_local_grib_expected)
    pd.DataFrame(lst_local_grib_remove, columns=["filename"]).to_csv(f"{str(work_dir)}/lst_local_grib_remove.csv", index=False)
    for i in lst_local_grib_remove:
        i_path = Path(i)
        if i_path.exists():
            i_path.unlink()

    return lst_local_grib_added, lst_local_grib_expected

if __name__ == "__main__":
    download_hrrr_live()
