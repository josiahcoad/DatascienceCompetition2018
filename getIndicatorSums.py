# pylint: disable=missing-docstring, invalid-name
"""Newest version. Get indicator proportions for iAirToSub and iNotAirToSub."""

import datetime as dt
import time
from multiprocessing import Pool
import numpy as np
import pandas as pd
import shapely.wkt
from shapely.geometry.polygon import Polygon


DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object,
    "Pickup O'Hare Community Area": float, "Dropoff Centroid  Location": object
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


def getdaynum(string):
    month, day, year = map(int, [string[:2], string[3:5], string[6:10]])
    return dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday


def addDays(df):
    df["day"] = df["Trip Start Timestamp"].map(getdaynum)
    return df


def getDowntownBoundary():
    x1 = (-87.668046, 41.925163)
    x2 = (-87.632791, 41.925714)
    x3 = (-87.613650, 41.892051)
    x4 = (-87.611171, 41.852618)
    x5 = (-87.660700, 41.851470)
    return Polygon([x1, x2, x3, x4, x5])


def getSuburbBoundary():
    x1 = (-88.040402, 42.072566)
    x2 = (-87.697086, 42.078652)
    x3 = (-87.532285, 41.746548)
    x4 = (-87.528165, 41.603970)
    x5 = (-87.686093, 41.623477)
    x6 = (-87.804883, 41.729640)
    return Polygon([x1, x2, x3, x4, x5, x6])


def getInPolygonIndicators(wktdata, polygon):
    points = wktdata.map(shapely.wkt.loads)
    return points.map(polygon.contains) * 1


def addDropoffSuburbIndicators(df):
    df["iDropoffSuburb"] = getInPolygonIndicators(df["Dropoff Centroid  Location"],
                                                  getSuburbBoundary())
    return df


def addNAtoSIndicators(df):
    df["iNotAirToSub"] = ((1 - df["iDropoffSuburb"]) *
                          df["Pickup O'Hare Community Area"])
    return df


def addAtoSIndicators(df):
    df["iAirToSub"] = df["iDropoffSuburb"] * df["Pickup O'Hare Community Area"]
    return df


def readWrite(year):
    basepath = "."
    datapath = f"{basepath}/original"
    outsumspath = f"{basepath}/daily/sums/{year}"
    outmedianpath = f"{basepath}/daily/medians"

    filename = f"{datapath}/Chicago_taxi_trips{year}.csv"
    t0 = time.time()
    df = pd.read_csv(filename,
                     usecols=DATATYPES.keys(),
                     dtype=DATATYPES,
                     nrows=10000)
    print(f"{filename} read in {round((time.time()-t0)/60, 2)} min.")

    df = parallelize_dataframe(df, addDays)
    print(f"Days added in {round((time.time()-t0)/60, 2)} min.")

    df = parallelize_dataframe(df, addDropoffSuburbIndicators)
    print(f"DropoffSub added in {round((time.time()-t0)/60, 2)} min.")

    df = parallelize_dataframe(df, addNAtoSIndicators)
    print(f"NAtoS added in {round((time.time()-t0)/60, 2)} min.")

    df = parallelize_dataframe(df, addAtoSIndicators)
    print(f"AtoS added in {round((time.time()-t0)/60, 2)} min.")

    medians = pd.DataFrame()
    readcols = ["iNotAirToSub", "iAirToSub"]

    for column in readcols:
        groups = df.groupby(["Taxi ID", "week"])[column]
        result = (groups.sum() / groups.count()).unstack(level=-1)

        column = column.replace(" ", "_")
        medians[column] = result.median()
        result.to_csv(f"{outsumspath}/{column}_{year}_sums.csv", index=False)
        print(f"{outsumspath}/{column}_{year} in {round((time.time()-t0)/60, 2)} min.")

    medians.to_csv(f"{outmedianpath}/medians_{year}.csv",
                   index_label="day", header=readcols)
    print(f"Done in total {round((time.time()-t0)/60, 2)} min.")


if __name__ == "__main__":
    # for i in range(2013, 2018):
    #     readWrite(i)
    readWrite(2017)
