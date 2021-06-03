"""main program, download raw files, and depending on whether there are new files,
extract new files, merge and upload to database"""
from download_hrrr_live import download_hrrr_live
from extract_grib_csv import extract_grib_csv
import os
from pathlib import Path
import pandas as pd
from utils import storm_dir, filter_hours, get_lst_diff

work_dir = storm_dir/"HRRR_live"
path_out = work_dir/"CSV"
sas_exe = Path("")
merge_script = work_dir/"merge_csv_live.sh"
upload_script = work_dir/"upload_hrrr_live.sas"
upload_log = (work_dir/upload_script.stem).with_suffix(".log")

if __name__ == "__main__":
    lst_local_grib_added, lst_local_grib_expected = download_hrrr_live()
    lst_local_grib_added2 = list(filter(filter_hours, lst_local_grib_added))
    lst_local_grib_added2.sort()

    # proceed only if there are new files
    if len(lst_local_grib_added2) > 0:
        extract_grib_csv(lst_local_grib_added2)

    # csv files to be removed
    # convert list of local grib2 files to list of CSV files
    lst_local_csv_expected = list(map(lambda x: str(path_out/Path(x).stem)+".csv", lst_local_grib_expected))
    lst_local_csv_expected.sort()
    lst_local_csv_expected2 = list(filter(filter_hours, lst_local_csv_expected))
    lst_local_csv_expected2.sort()

    lst_local_csv_actual = list(path_out.glob("*csv"))
    lst_local_csv_actual = list(map(str, lst_local_csv_actual))
    lst_local_csv_actual.sort()

    lst_local_csv_remove = get_lst_diff(lst_local_csv_actual, lst_local_csv_expected2)

    for i in lst_local_csv_remove:
        filename = Path(i)
        basename = filename.stem
        csv_path = Path(str(path_out/filename.stem)+".csv")
        if csv_path.exists():
            csv_path.unlink()

    if len(lst_local_grib_added2) > 0 or len(lst_local_csv_remove) > 0:
        os.system(f"bash {str(merge_script)}")
        os.system(f"{str(sas_exe)} -sysin {str(upload_script)} -log {str(upload_log)}")
