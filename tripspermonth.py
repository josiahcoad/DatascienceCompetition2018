# pylint: disable=missing-docstring, invalid-name
"""Get number of trips per month."""

import pandas as pd

DATATYPES = {"Trip Start Timestamp": object}

def readWrite(startyear):
    folder = "original"
    allyears = pd.DataFrame()
    for year in range(startyear, 2018):
        filename = f"{folder}/Chicago_taxi_trips{year}.csv"
        df = pd.read_csv(filename,
                            usecols=DATATYPES.keys(),
                            dtype=DATATYPES)
        print(f"{year} read.")
        df["month"] = df["Trip Start Timestamp"].map(lambda x: x[0:2])
        tripsPerMonth = df.groupby("month").size()
        allyears[year] = tripsPerMonth
        print(f"{year} done.")
    allyears.to_csv(f"predictors/trips_per_month.csv")

if __name__ == "__main__":
    readWrite(2017)
