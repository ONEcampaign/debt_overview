"""Get raw data and save to raw_data directory."""

from bblocks.data_importers import InternationalDebtStatistics

from scripts.config import Paths
from scripts.logger import logger
from scripts.utils import add_africa_values, timeout_30min

START_YEAR = 2000


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


if __name__ == "__main__":
    logger.info("Fetching raw data")

    # get_debt_stocks_data()
    # get_debt_service_data()
    # get_currency_composition_data()
    get_disbursements_data()

    logger.info("Successfully fetched all raw data.")
