import datetime as dt
import time
from multiprocessing import Pool
import numpy as np
import pandas as pd

def parallelize_dataframe(df, func):
    num_partitions = 100
    num_cores = 4
    df_split = np.array_split(df, num_partitions)
    pool = Pool(num_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df

def gethour(string):
    month, day, year = map(int, [string[:2], string[3:5], string[6:10]])
    amOrPm = string.split(" ")[2]
    hour = int(string.split(" ")[1].split(':')[0])
    if amOrPm == "AM" and hour == 12:
        hour = hour - 12
    elif amOrPm == "PM" and hour != 12:
        hour = int(string.split(" ")[1].split(':')[0]) + 12
    day = dt.datetime(year, month, day, 0, 0, 0).timetuple().tm_yday
    hour = hour + (day - 1) * 24
    return hour

def addHours(df):
    df["hour"] = df["Trip Start Timestamp"].map(gethour)
    return df

# def addDay(df):
#     df['day']=(df['hour']%168)//24 + 1

# def addHour(df):
#     df['hourofday'] = df['hour']%24 + 1

years = range(2013, 2018)
for year in years:
    filename = f"../original/Chicago_taxi_trips{year}.csv"
    DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object, "Trip Total": object, "Trip Miles":float, "Trip Seconds": float
    }
    df = pd.read_csv(filename,
                     usecols=DATATYPES.keys(),
                     dtype=DATATYPES)
    df = parallelize_dataframe(df, addHours)
    df['day']=(df['hour']%168)//24 + 1
    df['hourofday'] = df['hour']%24 + 1
    readcols = ['Trip Total', 'Trip Miles', 'Trip Seconds']
    for col in readcols:
        if DATATYPES[col] == object:
            df[col] = df[col].map(lambda x: x if type(x) == float else float(x[1:]))
        res = df.groupby(['Taxi ID','day','hourofday'])[col].sum().groupby(["day", "hourofday"]).median()
        res = pd.DataFrame(res)
        res.columns = ['value']
        res.to_csv(f'./{year}/{col}.csv')
    res_num_trips = df.groupby(['day','hourofday']).size()
    res_num_trips.to_csv(f'./{year}/num_trips.csv')