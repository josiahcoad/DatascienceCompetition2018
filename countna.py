# pylint: disable=missing-docstring, invalid-name
import pandas as pd
import time
DATATYPES = {
    "Taxi ID": object, "Trip Start Timestamp": object,
    "Trip Miles": float, "Trip Total": object, "Trip Seconds": float,
    "Tolls": object, "Fare": object, "Tips": object, "Extras": object
}

ifolder = "original"
ofolder = "predictors"
allyears = pd.DataFrame()
t0 = time.time()
for year in range(2013, 2018):
    filename = f"{ifolder}/Chicago_taxi_trips{year}.csv"
    df = pd.read_csv(filename,
                     usecols=DATATYPES.keys(),
                     dtype=DATATYPES)
    print(f"read {year} after {round((time.time() - t0) / 60)}")
    allyears[year] = df.isnull().sum()
    print(f"sum for {year} after {round((time.time() - t0) / 60)}")

print(allyears)
allyears.to_csv(f"{ofolder}/nacounts.csv")
print(f"total after {round((time.time() - t0) / 60)}")
