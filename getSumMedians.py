# pylint: disable=missing-docstring, invalid-name
"""Get weekly/daily/hourly sums per taxi and medians across all taxis
of things like trip totals and trip seconds."""

import datetime as dt
import time
from multiprocessing import Pool
import numpy as np
import pandas as pd


DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object,
    "Trip Miles": float, "Trip Total": object, "Trip Seconds": float,
    "Tolls": object, "Fare": object, "Tips": object, "Extras": object
}


def parallelize_dataframe(df, func):
    num_partitions = 100
    num_cores = 4
    df_split = np.array_split(df, num_partitions)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def getday(string):
    month, day, year = map(int, [string[:2], string[3:5], string[6:10]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday


def getwk(string):
    month, day, year = map(int, [string[:2], string[3:5], string[6:10]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday // 7


def gethour(string):
    month, day, year = map(int, [string[:2], string[3:5], string[6:10]])
    amOrPm = string.split(" ")[2]
    if amOrPm == "AM":
        hour = int(string.split(" ")[1].split(':')[0])
    else:
        hour = int(string.split(" ")[1].split(':')[0]) + 12
    day = dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday
    hour = hour + (day - 1) * 24
    return hour


def addDays(df):
    df["day"] = df["Trip Start Timestamp"].map(getday)
    return df


def addHours(df):
    df["hour"] = df["Trip Start Timestamp"].map(gethour)
    return df


def addWeeks(df):
    df["week"] = df["Trip Start Timestamp"].map(getwk)
    return df


def getSums(column, df):
    if DATATYPES[column] == object:
        df[column] = df[column].map(
            lambda x: x if type(x) == float else float(x[1:]))
    # total_count = df.groupby(["Taxi ID", "day"])[
    #     column].sum().unstack(level=-1)
    # total_count = df.groupby(["Taxi ID", "hour"])[
    #     column].sum().unstack(level=-1)
    total_count = df.groupby(["Taxi ID", "hour"])[
        column].sum().unstack(level=-1)
    return total_count


def readWrite(year):
    basepath = "."
    timechunk = "hourly"
    datapath = f"{basepath}/original"
    outsumspath = f"{basepath}/{timechunk}/sums/{year}"
    outmedianpath = f"{basepath}/{timechunk}/medians"

    filename = f"{datapath}/Chicago_taxi_trips{year}.csv"
    t0 = time.time()
    readcols = ["Trip Total", "Trip Miles",
                "Trip Seconds", "Fare", "Tolls", "Extras", "Tips"]
    df = pd.read_csv(filename,
                     usecols=DATATYPES.keys(),
                     dtype=DATATYPES, nrows=100)
    print(f"{filename} read after {round((time.time()-t0)/60)} min.")

    df = parallelize_dataframe(df, addHours)
    # df = parallelize_dataframe(df, addDays)
    # df = parallelize_dataframe(df, addHours)
    print(f"{timechunk} added after {round((time.time()-t0)/60)} min.")

    medians = pd.DataFrame()
    for column in readcols:
        result = getSums(column, df)
        column = column.replace(" ", "_")
        medians[column] = result.median()
        result.to_csv(f"{outsumspath}/{column}_{year}_sums.csv", index=False)
        print(
            f"{outsumspath}/{column}_{year}_sums.csv after total {round((time.time()-t0)/60)} min.")
    medians.to_csv(f"{outmedianpath}/medians_{year}.csv",
                   index_label="hour", header=readcols)
    print(f"Medians {year} done after {round((time.time()-t0)/60)} min.")


if __name__ == "__main__":
    t0 = time.time()
    for year in range(2013, 2018):
        readWrite(year)
    # readWrite(2017)
    print(f"All done after {round((time.time()-t0)/60)} min.")
