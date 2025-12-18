"""Module for chart creation"""

import json
from datetime import datetime

import pandas as pd
from bblocks import places
from bblocks.data_importers import InternationalDebtStatistics, get_dsa

from scripts.config import Paths
from scripts.logger import logger
from scripts.utils import custom_sort

LATEST_YEAR = 2024
START_YEAR = 2000
NUM_EST_YEARS = 6  # number of estimated years in debt service data


def chart_1() -> None:
    """Chart 1: Bar debt stocks

    Bar chart, debt stocks over time for debtors and creditors, broken down by debt type
    (bilateral, multilateral, bonds, commercial banks, other private)
    """

    df = pd.read_parquet(Paths.raw_data / "ids_debt_stocks.parquet")

    # Basic cleaning
    df = (
        df.loc[
            lambda d: d.year >= START_YEAR,
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

    # go through each debtor/creditor pair and if all the values are zero, drop the pair
    df = (
        df.groupby(["debtor_name", "creditor_name"])
        .filter(lambda d: d["value"].sum() != 0)
        .reset_index(drop=True)
    )

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

    (
        df.rename(
            columns={
                "debtor_name": "filter1_values",
                "year": "x_values",
                "creditor_name": "filter2_values",
                "bilateral": "y1",
                "multilateral": "y2",
                "bonds": "y3",
                "commercial banks": "y4",
                "other private": "y5",
            }
        )
        .assign(y_values=lambda d: d[["y1", "y2", "y3", "y4", "y5"]].values.tolist())
        .loc[:, ["filter1_values", "x_values", "filter2_values", "y_values"]]
        .to_json(
            Paths.output / "chart_1_chart.json", orient="records", date_format="iso"
        )
    )

    logger.info("Chart 1 created successfully")


def _get_debt_service_data() -> pd.DataFrame:
    """Helper function to get cleaned debt service data"""

    mapping = {
        "DT.AMT.PBND.CD": {"category": "bonds", "type": "principal"},
        "DT.AMT.BLAT.CD": {"category": "bilateral", "type": "principal"},
        "DT.AMT.PCBK.CD": {"category": "commercial banks", "type": "principal"},
        "DT.AMT.MLAT.CD": {"category": "multilateral", "type": "principal"},
        "DT.AMT.PROP.CD": {"category": "other private", "type": "principal"},
        "DT.INT.BLAT.CD": {"category": "bilateral", "type": "interest"},
        "DT.INT.MLAT.CD": {"category": "multilateral", "type": "interest"},
        "DT.INT.PBND.CD": {"category": "bonds", "type": "interest"},
        "DT.INT.PCBK.CD": {"category": "commercial banks", "type": "interest"},
        "DT.INT.PROP.CD": {"category": "other private", "type": "interest"},
    }

    df = pd.read_parquet(Paths.raw_data / "ids_debt_service.parquet")

    return (
        df.loc[
            lambda d: (d.year >= START_YEAR) & (d.year <= LATEST_YEAR + NUM_EST_YEARS),
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
        .assign(
            category=lambda d: d.indicator_code.map(lambda x: mapping[x]["category"]),
            type=lambda d: d.indicator_code.map(lambda x: mapping[x]["type"]),
        )
    )


def chart_2() -> None:
    """Chart 2: Bar total debt service"""

    df = _get_debt_service_data()

    # export data for download
    df.to_csv(Paths.output / "chart_2_download.csv", index=False)

    # Chart data

    # remove debtor/creditor pairs where all values are zero
    df = (
        df.groupby(["debtor_name", "creditor_name"])
        .filter(lambda d: d["value"].sum() != 0)
        .reset_index(drop=True)
    )

    df = (
        df.groupby(["year", "debtor_name", "creditor_name", "category"])
        .agg({"value": "sum"})
        .reset_index()
        .pivot(
            index=["debtor_name", "year", "creditor_name"],
            columns="category",
            values="value",
        )
        .reset_index()
        .pipe(
            custom_sort,
            {"debtor_name": "Low & middle income", "creditor_name": "All creditors"},
        )
        .reset_index(drop=True)
    )

    # export chart data
    df.to_csv(Paths.output / "chart_2_chart.csv", index=False)

    # json chart data

    (
        df.rename(
            columns={
                "debtor_name": "filter1_values",
                "year": "x_values",
                "creditor_name": "filter2_values",
                "bilateral": "y1",
                "multilateral": "y2",
                "bonds": "y3",
                "commercial banks": "y4",
                "other private": "y5",
            }
        )
        .assign(y_values=lambda d: d[["y1", "y2", "y3", "y4", "y5"]].values.tolist())
        .loc[:, ["filter1_values", "x_values", "filter2_values", "y_values"]]
        .to_json(
            Paths.output / "chart_2_chart.json", orient="records", date_format="iso"
        )
    )

    logger.info("Chart 2 created successfully")


def chart_3() -> None:
    """Chart 3: Currency composition of debt"""

    indicators = {
        "DT.CUR.USDL.ZS": "U.S. dollars",
        "DT.CUR.EURO.ZS": "Euro",
        "DT.CUR.SDRW.ZS": "SDR",
        "DT.CUR.JYEN.ZS": "Japanese yen",
        "DT.CUR.UKPS.ZS": "Pound sterling",
        # 'DT.CUR.MULC.ZS': 'Multiple currencies'
    }

    df = pd.read_parquet(Paths.raw_data / "ids_currency_composition.parquet")

    df = df.loc[
        lambda d: (d.value.notna()) & (d.counterpart_name == "World") & (d.year >= 2001)
    ]

    # export data for download
    df.to_csv(Paths.output / "chart_3_download.csv", index=False)

    # chart data
    df = (
        df.pivot(
            index=["entity_name", "year"], columns="indicator_code", values="value"
        )
        .assign(
            **{
                "All other currencies": lambda d: d.loc[
                    :,
                    [i for i in df.indicator_code.unique() if i not in indicators],
                ].sum(axis=1)
            }
        )
        .loc[:, [*list(indicators.keys()), "All other currencies"]]
        .rename(columns=indicators)
        .reset_index()
        .pipe(custom_sort, {"entity_name": "Low & middle income"})
    )

    df.to_csv(Paths.output / "chart_3_chart.csv", index=False)
    logger.info("Chart 3 created successfully")


def chart_4() -> None:
    """Chart 4: Debt service broken down by interest and principal"""

    df = _get_debt_service_data()

    # export data for download
    df.to_csv(Paths.output / "chart_4_download.csv", index=False)

    # chart data

    # remove debtor/creditor pairs where all values are zero
    df = (
        df.groupby(["debtor_name", "creditor_name"])
        .filter(lambda d: d["value"].sum() != 0)
        .reset_index(drop=True)
    )

    df = (
        df.groupby(["year", "debtor_name", "creditor_name", "type"])
        .agg({"value": "sum"})
        .reset_index()
        .pivot(
            index=["debtor_name", "year", "creditor_name"],
            columns="type",
            values="value",
        )
        .reset_index()
        .pipe(
            custom_sort,
            {"debtor_name": "Low & middle income", "creditor_name": "All creditors"},
        )
        .reset_index(drop=True)
        # reorder columns with principal first
        .loc[:, ["debtor_name", "year", "creditor_name", "interest", "principal"]]
    )

    df.to_csv(Paths.output / "chart_4_chart.csv", index=False)

    # json chart data
    (
        df.rename(
            columns={
                "debtor_name": "filter1_values",
                "year": "x_values",
                "creditor_name": "filter2_values",
                "principal": "y1",
                "interest": "y2",
            }
        )
        .assign(y_values=lambda d: d[["y1", "y2"]].values.tolist())
        .loc[:, ["filter1_values", "x_values", "filter2_values", "y_values"]]
        .to_json(
            Paths.output / "chart_4_chart.json", orient="records", date_format="iso"
        )
    )

    logger.info("Chart 4 created successfully")


def chart_5() -> None:
    """Chart 5: DSA map"""

    color_map = {
        "High": "#ff6224",
        "Moderate": "#f5be29",
        "Low": "#00c3d1",
        "In debt distress": "#73175a",
    }

    # fetch DSA data
    df = get_dsa()

    df = (
        df.loc[
            lambda d: d.risk_of_debt_distress.notna(),
            [
                "country_name",
                "risk_of_debt_distress",
                "latest_publication",
                "debt_sustainability_assessment",
            ],
        ]
        .assign(
            iso3_code=lambda d: places.resolve_places(
                d.country_name, to_type="iso3_code"
            )
        )
        .assign(
            latest_publication=lambda d: pd.to_datetime(
                d.latest_publication
            ).dt.strftime("%d %B %Y")
        )
    )

    # export data for download
    df.to_csv(Paths.output / "chart_5_download.csv", index=False)

    # chart
    df = df.assign(color=lambda d: d.risk_of_debt_distress.map(color_map))

    df.to_csv(Paths.output / "chart_5_chart.csv", index=False)

    logger.info("Chart 5 created successfully")


def key_stats() -> None:
    """Key statistics"""

    stats_dict = {}

    # debt GNI ratio
    val = (
        InternationalDebtStatistics()
        .get_data(
            "DT.DOD.DECT.GN.ZS",
            entity_code="LMY",
            start_year=LATEST_YEAR,
            end_year=LATEST_YEAR,
        )
        .loc[lambda d: d.counterpart_code == "WLD", "value"]
        .values[0]
    )

    stats_dict["debt_gni"] = f"{round(val, 2)}%"

    # total debt stock
    val = (
        pd.read_parquet(Paths.raw_data / "ids_debt_stocks.parquet")
        .loc[
            lambda d: (d.entity_code == "LMY")
            & (d.counterpart_code == "WLD")
            & (d.year == LATEST_YEAR)
        ]
        .value.sum()
        / 1_000_000_000_000
    )
    stats_dict["debt_stock_total"] = f"US${round(val, 2)} trillion"

    # total debt service
    val = (
        _get_debt_service_data()
        .loc[
            lambda d: (d.debtor_name == "Low & middle income")
            & (d.creditor_name == "All creditors")
            & (d.year == LATEST_YEAR)
        ]
        .value.sum()
        / 1_000_000_000
    )

    stats_dict["debt_service_total"] = f"US${round(val, 2)} billion"

    # countries in debt distress
    val = len(
        get_dsa().loc[
            lambda d: d.risk_of_debt_distress.isin(["In debt distress", "High"])
        ]
    )

    stats_dict["countries_debt_distress"] = val

    stats_dict["latest_year"] = LATEST_YEAR  # latest year of data

    with open(Paths.output / "key_stats.json", "w") as f:
        json.dump(stats_dict, f)


def last_update() -> None:
    """Set the last update date for the analysis
    in the key_stats.json file without overriding other kv pairs
    """

    with open(Paths.output / "key_stats.json", "r+") as f:
        key_stats_dict = json.load(f)
        key_stats_dict["last_data_update"] = datetime.now().strftime("%d %B %Y")

        # replace the file content
        f.seek(0)
        json.dump(key_stats_dict, f)
        f.truncate()

    logger.info("Updated last data update date")


if __name__ == "__main__":
    logger.info("Running charts and key statistics")

    chart_1()  # debt stocks chart
    chart_2()  # total debt service chart
    chart_3()  # debt composition chart
    chart_4()  # debt service by interest and principal chart
    chart_5()  # DSA map chart
    key_stats()  # key statistics
    last_update()  # last update date

    logger.info("Successfully created all charts")
