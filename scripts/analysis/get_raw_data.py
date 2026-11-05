"""Get raw data and save to raw_data directory."""

import os

import pandas as pd
from bblocks.data_importers import InternationalDebtStatistics
from bblocks.data_importers.config import (
    DataExtractionError,
    DataFormattingError,
    DataValidationError,
)
from bblocks.data_importers.data_validators import DataFrameValidator
from bblocks.data_importers.imf import dsa as imf_dsa

from scripts.config import Paths
from scripts.logger import logger
from scripts.utils import add_africa_values, timeout_30min

START_YEAR = 2000

# Tracked in git, so the analysis can still run when the download fails
DSA_PATH = Paths.raw_data / "imf_dsa.csv"

# Columns of the DSA list used by the charts
DSA_COLUMNS = [
    "country_name",
    "latest_publication",
    "risk_of_debt_distress",
    "debt_sustainability_assessment",
]


@timeout_30min
def get_debt_stocks_data() -> None:
    """Get the raw data for the International Debt Statistics."""

    ids = InternationalDebtStatistics()
    inds = list(ids.debt_stock_indicators.indicator_code.unique())

    df = ids.get_data(inds, include_labels=True, start_year=START_YEAR)

    # add Africa values
    df = add_africa_values(df, agg_operation="sum")

    df.to_parquet(Paths.raw_data / "ids_debt_stocks.parquet", index=False)

    logger.info("IDS debt stocks data downloaded successfully.")


@timeout_30min
def get_debt_service_data() -> None:
    """Get the raw data for the International Debt Statistics total debt service."""

    ids = InternationalDebtStatistics()
    inds = list(ids.debt_service_indicators.indicator_code.unique())

    df = ids.get_data(inds, include_labels=True, start_year=START_YEAR)

    # add Africa values
    df = add_africa_values(df, agg_operation="sum")

    df.to_parquet(Paths.raw_data / "ids_debt_service.parquet", index=False)

    logger.info("IDS total debt service data downloaded successfully.")


@timeout_30min
def get_currency_composition_data() -> None:
    """Get the raw data for the International Debt Statistics currency composition."""

    ids = InternationalDebtStatistics()
    cc_vars = (
        ids.get_available_indicators()
        .loc[lambda d: d["indicator_code"].str.contains("DT.CUR"), "indicator_code"]
        .to_list()
    )

    df = ids.get_data(cc_vars, include_labels=True, start_year=START_YEAR)
    df.to_parquet(Paths.raw_data / "ids_currency_composition.parquet", index=False)

    logger.info("IDS currency composition data downloaded successfully.")


def get_disbursements_data() -> None:
    """Get the raw data for the International Debt Statistics disbursements."""

    ids = InternationalDebtStatistics()

    disbursement_vars = [
        "DT.DIS.BLAT.CD",
        "DT.DIS.MLAT.CD",
        "DT.DIS.PBND.CD",
        "DT.DIS.PCBK.CD",
        "DT.DIS.PROP.CD",
    ]

    df = ids.get_data(disbursement_vars, include_labels=True, start_year=START_YEAR)
    df = add_africa_values(df, agg_operation="sum")
    df.to_parquet(Paths.raw_data / "ids_disbursements.parquet", index=False)

    logger.info("IDS disbursements data downloaded successfully.")


def parse_dsa_pdf(content: bytes) -> pd.DataFrame:
    """Parse the IMF list of LIC DSAs from its PDF.

    Uses the bblocks-data-importers parser, whose get_dsa() fails on the current
    layout of the PDF (as of September 2026). The PDF no longer has the "joint
    with World Bank" and "latest DSA discussed" columns, and shows "…" where no
    debt sustainability assessment applies. Only the columns the charts use are
    kept. This relies on the importer's private functions, so check it still
    works after upgrading bblocks-data-importers.
    """

    df = imf_dsa._clean_df(imf_dsa._pdf_to_df(content))
    DataFrameValidator().validate(df, DSA_COLUMNS)

    return df.loc[:, DSA_COLUMNS].replace("…", pd.NA)


def get_dsa_data() -> None:
    """Get the IMF list of LIC debt sustainability assessments (DSAs).

    If the download fails, log a warning and keep the copy saved in the repo.
    The IMF currently blocks scripted downloads of the list
    (ONEcampaign/bblocks_data_importers#102). To update the saved copy by hand,
    see scripts/analysis/update_dsa.py.
    """

    try:
        df = parse_dsa_pdf(imf_dsa._download_pdf(imf_dsa.URL))
    except (DataExtractionError, DataFormattingError, DataValidationError) as e:
        if not DSA_PATH.exists():
            raise
        message = (
            "Could not download the IMF DSA list, using the saved copy in "
            f"{DSA_PATH.relative_to(Paths.project)}. Error: {e}"
        )
        logger.warning(message)
        if os.environ.get("GITHUB_ACTIONS") == "true":
            # Show the warning on the workflow run's summary page
            print(f"::warning title=IMF DSA list not updated::{message}")
        return

    df.to_csv(DSA_PATH, index=False)

    logger.info("IMF DSA list downloaded successfully.")


if __name__ == "__main__":
    logger.info("Fetching raw data")

    get_debt_stocks_data()
    get_debt_service_data()
    get_currency_composition_data()
    get_disbursements_data()
    get_dsa_data()

    logger.info("Successfully fetched all raw data.")
