"""Module for chart creation"""

import pandas as pd

from scripts.config import Paths
from scripts.logger import logger
from scripts.utils import custom_sort


def chart_1() -> None:
    """Chart 1: Bar debt stocks

    Bar chart, debt stocks over time for debtors and creditors, broken down by debt type
    (bilateral, multilateral, bonds, commercial banks, other private)
    """

    df = pd.read_parquet(Paths.raw_data / "ids_debt_stocks.parquet")

    # Basic cleaning
    df = (
        df.loc[
            lambda d: d.year >= 2000,
            [
                "indicator_name",
                "indicator_code",
                "year",
                "entity_name",
                "counterpart_name",
                "value",
            ],
        ]
        .dropna(subset=["value"])
        .assign(
            counterpart_name=lambda d: d.counterpart_name.replace(
                {"World": "All creditors"}
            )
        )
        .rename(
            columns={"entity_name": "debtor_name", "counterpart_name": "creditor_name"}
        )
    )

    # export data for download
    df.to_csv(Paths.output / "chart_1_download.csv", index=False)

    # Chart data

    cols_map = {
        "DT.DOD.BLAT.CD": "bilateral",
        "DT.DOD.MLAT.CD": "multilateral",
        "DT.DOD.PBND.CD": "bonds",
        "DT.DOD.PCBK.CD": "commercial banks",
        "DT.DOD.PROP.CD": "other private",
    }

    df = (
        df.pivot(
            index=["debtor_name", "year", "creditor_name"],
            columns="indicator_code",
            values="value",
        )
        .reset_index()
        .rename(columns=cols_map)
        .pipe(
            custom_sort,
            {"debtor_name": "Low & middle income", "creditor_name": "All creditors"},
        )
        .reset_index(drop=True)
    )

    # export chart data
    df.to_csv(Paths.output / "chart_1_chart.csv", index=False)


    # create json data for chart

    (df
     .rename(columns={"debtor_name": "filter1_values",
                      "year": "x_values",
                      "creditor_name": "filter2_values",
                      "bilateral": "y1",
                      "multilateral": "y2",
                      "bonds": "y3",
                      "commercial banks": "y4",
                      "other private": "y5",
                      })
     .assign(y_values=lambda d: d[["y1", "y2", "y3", "y4", "y5"]].values.tolist())
     .loc[:, ["filter1_values", "x_values", "filter2_values", "y_values"]]
     .to_json(Paths.output / "chart_1_chart.json", orient="records", date_format="iso")
     )

    logger.info("Chart 1 created successfully")


def chart_2() -> None:
    """Chart 2: Bar total debt service"""

    df = pd.read_parquet(Paths.raw_data / "ids_total_debt_service.parquet")

    # Basic cleaning

    df = (
        df.loc[
            lambda d: d.year >= 2000,
            [
                "indicator_name",
                "indicator_code",
                "year",
                "entity_name",
                "counterpart_name",
                "value",
            ],
        ]
        .dropna(subset=["value"])
        .assign(
            counterpart_name=lambda d: d.counterpart_name.replace(
                {"World": "All creditors"}
            )
        )
        .rename(
            columns={"entity_name": "debtor_name", "counterpart_name": "creditor_name"}
        )
        .reset_index(drop=True)
    )

    # export data for download
    df.to_csv(Paths.output / "chart_2_download.csv", index=False)

    # Chart data

    cols_map = {
        "DT.TDS.BLAT.CD": "bilateral",
        "DT.TDS.MLAT.CD": "multilateral",
        "DT.TDS.PBND.CD": "bonds",
        "DT.TDS.PCBK.CD": "commercial banks",
        "DT.TDS.PROP.CD": "other private",
    }

    df = (
        df.pivot(
            index=["debtor_name", "year", "creditor_name"],
            columns="indicator_code",
            values="value",
        )
        .reset_index()
        .rename(columns=cols_map)
        .pipe(
            custom_sort,
            {"debtor_name": "Low & middle income", "creditor_name": "All creditors"},
        )
        .reset_index(drop=True)
    )

    # export chart data
    df.to_csv(Paths.output / "chart_2_chart.csv", index=False)


    # json chart data

    (df
     .rename(columns={"debtor_name": "filter1_values",
                      "year": "x_values",
                      "creditor_name": "filter2_values",
                      "bilateral": "y1",
                      "multilateral": "y2",
                      "bonds": "y3",
                      "commercial banks": "y4",
                      "other private": "y5",
                      })
     .assign(y_values=lambda d: d[["y1", "y2", "y3", "y4", "y5"]].values.tolist())
     .loc[:, ["filter1_values", "x_values", "filter2_values", "y_values"]]
     .to_json(Paths.output / "chart_2_chart.json",
              orient="records", date_format="iso")
     )

    logger.info("Chart 2 created successfully")


if __name__ == "__main__":
    chart_1()
    chart_2()

    logger.info("Successfully created all charts")
