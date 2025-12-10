"""Module for chart creation"""

import pandas as pd

from scripts.config import Paths
from scripts.logger import logger
from scripts.utils import custom_sort


def chart_1():
    """Chart 1: Debt stocks"""

    df = pd.read_parquet(Paths.raw_data / "ids_debt_stocks.parquet")

    # Basic cleaning
    df = (df
     .loc[lambda d: d.year >= 2000, ["indicator_name", "indicator_code",
                                     "year", "entity_name", "counterpart_name", "value",
                                     ]
     ]
     .dropna(subset=["value"])

     .assign(counterpart_name=lambda d: d.counterpart_name.replace({"World": "All creditors"}))
     .rename(columns={"entity_name": "debtor_name", "counterpart_name": "creditor_name"})

     )

    # export data for download
    df.to_csv(Paths.output / "chart_1_download.csv", index=False)

    # Chart data

    cols_map = {'DT.DOD.BLAT.CD': 'bilateral',
            'DT.DOD.MLAT.CD': 'multilateral',
            'DT.DOD.PBND.CD': 'bonds',
            'DT.DOD.PCBK.CD': 'commercial banks',
            'DT.DOD.PROP.CD': 'other private'}

    df = (df
     .pivot(index=["debtor_name", "year", "creditor_name"],
            columns='indicator_code',
            values='value',
            )
     .reset_index()
     .rename(columns=cols_map)
    .pipe(custom_sort, {"debtor_name": "Low & middle income", "creditor_name": "All creditors"})
     .reset_index(drop=True)

     )

    # export chart data
    df.to_csv(Paths.output / "chart_1_chart.csv", index=False)

    logger.info("Chart 1 created successfully")


if __name__ == "__main__":

    chart_1()

    logger.info("Successfully created all charts")









