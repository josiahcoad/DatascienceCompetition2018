# pylint: disable=missing-docstring, invalid-name
"""Get weekly/daily/hourly total sums for a given timechunk.
Assumes you already ran getSumMedians on the original data."""

import os
import pandas as pd


if __name__ == "__main__":
    timechunk = "daily"
    allyears = []
    for year in range(2013, 2018):
        sumsfolderpath = f"{timechunk}/sums/{year}"
        yeartotals = pd.DataFrame()
        for infile in os.listdir(sumsfolderpath):
            colname = infile.split(f"_{year}")[0]
            yeartotals[colname] = pd.read_csv(f"{sumsfolderpath}/{infile}").sum()
        allyears.append(yeartotals)
        yeartotals.to_csv(f"{timechunk}/totals/total_{year}.csv")
    pd.concat(allyears).to_csv(f"{timechunk}/totals/total_all.csv")
