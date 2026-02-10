"""Module for chart creation"""

import json
from datetime import datetime

import pandas as pd
import unesco_reader as uis
from bblocks import places
from bblocks.data_importers import GHED, InternationalDebtStatistics, get_dsa

from scripts.config import Paths
from scripts.logger import logger
from scripts.utils import custom_sort, format_values, get_gov_expenditure_curr_usd

LATEST_YEAR = 2024
START_YEAR = 2000
NUM_EST_YEARS = 6  # number of estimated years in debt service data
GHED_END_YEAR = (
    2023  # latest year for GHED data NOTE: to be updated with new releases!!!!!
)


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


def chart_6() -> None:
    """Chart 6: Packed circle chart total debt stocks latest value"""

    cols_map = {
        "DT.DOD.BLAT.CD": "bilateral",
        "DT.DOD.MLAT.CD": "multilateral",
        "DT.DOD.PBND.CD": "bonds",
        "DT.DOD.PCBK.CD": "commercial banks",
        "DT.DOD.PROP.CD": "other private",
    }

    df = pd.read_parquet(Paths.raw_data / "ids_debt_stocks.parquet")

    df = (
        df.loc[lambda d: d.year == LATEST_YEAR,]
        .dropna(subset=["value"])
        .assign(category=lambda d: d.indicator_code.map(cols_map))
        .loc[
            :,
            ["counterpart_name", "entity_name", "category", "indicator_code", "value"],
        ]
        .loc[lambda d: d.counterpart_name != "World"]
        .groupby(["counterpart_name", "entity_name", "category"])
        .agg({"value": "sum"})
        .reset_index()
        .pipe(custom_sort, {"entity_name": ["Low & middle income"]})
    )

    # save chart data
    df.to_csv(Paths.output / "chart_6_download.csv", index=False)

    (
        df.assign(value_annotation=lambda d: d.value.apply(format_values)).to_csv(
            Paths.output / "chart_6_chart.csv", index=False
        )
    )

    logger.info("Chart 6 created successfully")


def chart_7() -> None:
    """Chart 7: Debt disbursements"""

    df = pd.read_parquet(Paths.raw_data / "ids_disbursements.parquet")

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

    # go through each debtor/creditor pair and if all the values are zero, drop the pair
    df = (
        df.groupby(["debtor_name", "creditor_name"])
        .filter(lambda d: d["value"].sum() != 0)
        .reset_index(drop=True)
    )

    cols_map = {
        "DT.DIS.BLAT.CD": "bilateral",
        "DT.DIS.MLAT.CD": "multilateral",
        "DT.DIS.PBND.CD": "bonds",
        "DT.DIS.PCBK.CD": "commercial banks",
        "DT.DIS.PROP.CD": "other private",
    }

    # export data for download
    df.to_csv(Paths.output / "chart_7_download.csv", index=False)

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
    df.to_csv(Paths.output / "chart_7_chart.csv", index=False)

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
            Paths.output / "chart_7_chart.json", orient="records", date_format="iso"
        )
    )

    logger.info("Chart 7 created successfully")


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


def chart_8() -> None:
    """Chart 8: Line chart compare debt service (% of gov expenditure) to education and health"""

    gov_exp = get_gov_expenditure_curr_usd().rename(
        columns={"value": "gov_expenditure_usd"}
    )
    ds = _get_debt_service_data()

    # combine debt service for all creditors and calculate debt service to gov expenditure ratio
    df = (
        ds.loc[lambda d: d.creditor_name == "All creditors"]
        .groupby(["debtor_name", "year"], as_index=False)
        .agg({"value": "sum"})
        .assign(
            entity_code=lambda d: places.resolve_places(
                d.debtor_name, to_type="iso3_code", not_found="ignore"
            )
        )
        .merge(gov_exp, how="left")
        .assign(**{"debt service": lambda d: d.value / d.gov_expenditure_usd * 100})
        .dropna(subset=["debt service"])
        .drop(columns=["gov_expenditure_usd", "value"])
        .loc[lambda d: d.year <= LATEST_YEAR]
    )

    # health expenditure data from GHED
    health_data = (
        GHED()
        .get_data()
        .loc[
            lambda d: (d.indicator_code == "gghed_gge") & (d.year <= GHED_END_YEAR),
            ["iso3_code", "value", "year"],
        ]
        .rename(columns={"iso3_code": "entity_code", "value": "health"})
    )

    # education expenditure data from UIS
    education_data = (
        uis.get_data("XGOVEXP.IMF")
        .loc[:, ["geoUnit", "year", "value"]]
        .rename(columns={"geoUnit": "entity_code", "value": "education"})
    )

    # merge all data
    df = df.merge(health_data, how="left").merge(  # merge health data
        education_data, how="left"
    )  # merge education data

    # add world and Africa median
    world_median = (
        df.groupby("year", as_index=False)
        .agg({"debt service": "median", "health": "median", "education": "median"})
        .assign(debtor_name="Low & middle income (median)")
    )

    africa_median = (
        df.assign(
            region=lambda d: places.resolve_places(
                d.entity_code, from_type="iso3_code", to_type="region"
            )
        )
        .loc[lambda d: d.region == "Africa"]
        .groupby("year", as_index=False)
        .agg({"debt service": "median", "health": "median", "education": "median"})
        .assign(debtor_name="Africa (excluding high income) (median)")
    )

    df = pd.concat([df, world_median, africa_median], ignore_index=True)

    df = custom_sort(
        df,
        {
            "debtor_name": [
                "Low & middle income (median)",
                "Africa (excluding high income) (median)",
            ]
        },
    )

    # export data for download
    df.to_csv(Paths.output / "chart_8_download.csv", index=False)

    # chart data
    df.to_csv(Paths.output / "chart_8_chart.csv", index=False)

    logger.info("Chart 8 created successfully")


def _add_low_lower_middle_income(
    df: pd.DataFrame,
    debtor_col: str = "entity_name",
    index_cols=None,
    value_col: str = "value",
) -> pd.DataFrame:
    """Helper function to aggregate data for low and lower middle income countries
    and add it to the main dataframe

    """

    # aggregate for debtor Low and lower middle income
    if index_cols is None:
        index_cols = ["year", "indicator_name", "counterpart_name"]

    llmi_df = (
        df.loc[lambda d: d[debtor_col].isin(["Low income", "Lower middle income"])]
        .groupby(index_cols, observed=True)
        .agg({value_col: "sum"})
        .reset_index()
        .assign(**{debtor_col: "Low & lower middle income"})
    )

    # add the low and lower middle income aggregate to the main df
    return pd.concat([df, llmi_df], ignore_index=True)


def _calculate_china_and_other_proportion(df: pd.DataFrame) -> pd.DataFrame:
    """ """

    # aggregate for creditor: China bilateral and private
    china_df = (
        df.loc[lambda d: d.counterpart_name == "China"]
        .groupby(["year", "indicator_name", "entity_name"], observed=True)
        .agg({"value": "sum"})
        .reset_index()
        .assign(indicator_name=lambda d: "China " + "(" + d.indicator_name + ")")
    )

    # aggregate for total of all other creditors
    other_df = (
        df.loc[lambda d: ~d.counterpart_name.isin(["World", "China"])]
        .groupby(["year", "entity_name"], observed=True)
        .agg({"value": "sum"})
        .reset_index()
        .assign(indicator_name="Other creditors")
    )

    combined_df = pd.concat([china_df, other_df], ignore_index=True)

    # total debt stock (World) per debtor and year
    total_df = (
        df.loc[lambda d: d.counterpart_name == "World"]
        .groupby(["year", "entity_name"], observed=True)
        .agg(total_value=("value", "sum"))
        .reset_index()
    )

    # convert each creditor value to % of total
    combined_df = (
        combined_df.merge(total_df, on=["year", "entity_name"], how="left")
        .assign(value=lambda d: 100 * d.value / d.total_value)
        .drop(columns="total_value")
    )

    return combined_df


def _cleaning_china_chart_data(df: pd.DataFrame, cols_map: dict) -> pd.DataFrame:
    """helper function to clean data for China proportion charts"""

    # Basic cleaning
    return (
        df.dropna(subset=["value"])
        .loc[lambda d: d.year >= START_YEAR]
        .assign(indicator_name=lambda d: d.indicator_code.map(cols_map))
        .loc[:, ["entity_name", "year", "value", "indicator_name", "counterpart_name"]]
        # drop any entity counterpart combinations where the value is 0 for all years (e.g. no stocks at all)
        .groupby(["entity_name", "counterpart_name", "indicator_name"], as_index=False)
        .filter(lambda d: d.value.ne(0).any())
    )


def chart_9() -> None:
    """Chart 9: bar chart, China bilateral vs private vs other creditors"""

    df = pd.read_parquet(Paths.raw_data / "ids_debt_stocks.parquet")

    cols_map = {
        "DT.DOD.BLAT.CD": "bilateral",
        "DT.DOD.MLAT.CD": "multilateral",
        # all private categories grouped together
        "DT.DOD.PBND.CD": "private",
        "DT.DOD.PCBK.CD": "private",
        "DT.DOD.PROP.CD": "private",
    }

    # Basic cleaning
    df = _cleaning_china_chart_data(df, cols_map)
    df = _add_low_lower_middle_income(df)
    combined_df = _calculate_china_and_other_proportion(df)
    combined_df = combined_df.rename(
        columns={"indicator_name": "creditor_name", "entity_name": "debtor_name"}
    )

    # export data for download
    combined_df.to_csv(Paths.output / "chart_9_download.csv", index=False)

    # chart data
    (
        combined_df.pivot(
            index=["debtor_name", "year"], columns="creditor_name", values="value"
        )
        .reset_index()
        .pipe(
            custom_sort,
            {
                "debtor_name": [
                    "Low & middle income",
                    "Low & lower middle income",
                    "Africa (excluding high income)",
                ]
            },
        )
        # keep only entity_name where at least one of the China columns is not null
        .loc[lambda d: d.filter(like="China").notna().any(axis=1)]
        .to_csv(Paths.output / "chart_9_chart.csv", index=False)
    )


def chart_10() -> None:
    """Chart 10: China proportion of debt disbursements"""

    df = pd.read_parquet(Paths.raw_data / "ids_disbursements.parquet")

    cols_map = {
        "DT.DIS.BLAT.CD": "bilateral",
        "DT.DIS.MLAT.CD": "multilateral",
        # all private categories grouped together
        "DT.DIS.PBND.CD": "private",
        "DT.DIS.PCBK.CD": "private",
        "DT.DIS.PROP.CD": "private",
    }

    # Basic cleaning
    df = _cleaning_china_chart_data(df, cols_map)
    df = _add_low_lower_middle_income(df)
    combined_df = _calculate_china_and_other_proportion(df)
    combined_df = combined_df.rename(
        columns={"indicator_name": "creditor_name", "entity_name": "debtor_name"}
    )

    # remove estimate years
    combined_df = combined_df.loc[lambda d: d.year <= LATEST_YEAR]

    # export data for download
    combined_df.to_csv(Paths.output / "chart_10_download.csv", index=False)

    # chart data
    (
        combined_df.pivot(
            index=["debtor_name", "year"], columns="creditor_name", values="value"
        )
        .reset_index()
        .pipe(
            custom_sort,
            {
                "debtor_name": [
                    "Low & middle income",
                    "Low & lower middle income",
                    "Africa (excluding high income)",
                ]
            },
        )
        # keep only entity_name where at least one of the China columns is not null
        .loc[lambda d: d.filter(like="China").notna().any(axis=1)]
        .to_csv(Paths.output / "chart_10_chart.csv", index=False)
    )


def chart_11() -> None:
    """Chart 11: China proportion of debt service"""

    cols_map = {
        "DT.AMT.PBND.CD": "private",
        "DT.AMT.BLAT.CD": "bilateral",
        "DT.AMT.PCBK.CD": "private",
        "DT.AMT.MLAT.CD": "multilateral",
        "DT.AMT.PROP.CD": "private",
        "DT.INT.BLAT.CD": "bilateral",
        "DT.INT.MLAT.CD": "multilateral",
        "DT.INT.PBND.CD": "private",
        "DT.INT.PCBK.CD": "private",
        "DT.INT.PROP.CD": "private",
    }

    df = pd.read_parquet(Paths.raw_data / "ids_debt_service.parquet")

    # Basic cleaning
    df = _cleaning_china_chart_data(df, cols_map)
    df = _add_low_lower_middle_income(df)
    combined_df = _calculate_china_and_other_proportion(df)
    combined_df = combined_df.rename(
        columns={"indicator_name": "creditor_name", "entity_name": "debtor_name"}
    )

    # export data for download
    combined_df.to_csv(Paths.output / "chart_11_download.csv", index=False)

    # chart data
    (
        combined_df.pivot(
            index=["debtor_name", "year"], columns="creditor_name", values="value"
        )
        .reset_index()
        .pipe(
            custom_sort,
            {
                "debtor_name": [
                    "Low & middle income",
                    "Low & lower middle income",
                    "Africa (excluding high income)",
                ]
            },
        )
        # keep only entity_name where at least one of the China columns is not null
        .loc[lambda d: d.filter(like="China").notna().any(axis=1)]
        .to_csv(Paths.output / "chart_11_chart.csv", index=False)
    )


if __name__ == "__main__":
    logger.info("Running charts and key statistics")

    chart_1()  # debt stocks chart
    chart_2()  # total debt service chart
    chart_3()  # debt composition chart
    chart_4()  # debt service by interest and principal chart
    chart_5()  # DSA map chart
    chart_6()  # packed circle chart
    chart_7()  # debt disbursements chart
    chart_8()  # debt service vs social expenditure chart
    chart_9()  # China bilateral vs private vs other creditors chart

    key_stats()  # key statistics
    last_update()  # last update date

    # temporary chart objects not embedded in page
    chart_10()  # China proportion of debt disbursements chart
    chart_11()  # China proportion of debt service chart

    logger.info("Successfully created all charts")
