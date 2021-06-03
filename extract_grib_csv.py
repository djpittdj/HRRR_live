from process_grib import process_grib
import pandas as pd
from utils import *
from multiprocessing import Pool
import glob
from pathlib import Path

work_dir = storm_dir/"HRRR_live"
path_in = work_dir/"GRIB2"
path_out = work_dir/"CSV"

df_unique_grid = pd.read_csv(f"{storm_dir}/data_GIS/unique_grid_id.csv")

def extract_grib_csv_one(filename, path_out):
    df = process_grib(filename, df_unique_grid)
    basename = filename.stem
    df.to_csv(f"{path_out}/{basename}.csv", index=False)

# def func(filename):
#     extract_grib_csv_one(filename, path_out)

def extract_grib_csv(lst_local_added2):
    for i in lst_local_added2:
        filename = Path(i)
        if filename.exists():
            extract_grib_csv_one(filename, path_out)

    # parallel version
    # with Pool(processes=8) as pool:
    #     for i in filenames:
    #         result = pool.apply_async(func, (i,))
    #         result.get()

if __name__ == "__main__":
    extract_grib_csv()
