"""Get raw data"""

from bblocks.data_importers import InternationalDebtStatistics

from scripts.config import Paths
from scripts.logger import logger
from scripts.utils import timeout_30min

TDS_VARS = [
    "DT.TDS.BLAT.CD",
    "DT.TDS.MLAT.CD",
    "DT.TDS.PBND.CD",
    "DT.TDS.PCBK.CD",
    "DT.TDS.PROP.CD",
]


@timeout_30min
def get_debt_stocks_data() -> None:
    """Get the raw data for the International Debt Statistics."""

    ids = InternationalDebtStatistics()
    inds = list(ids.debt_stock_indicators.indicator_code.unique())

    df = ids.get_data(inds, include_labels=True)
    df.to_parquet(Paths.raw_data / "ids_debt_stocks.parquet", index=False)

    logger.info("IDS debt stocks data downloaded successfully.")


@timeout_30min
def get_total_debt_service_data() -> None:
    """Get the raw data for the International Debt Statistics total debt service."""

    ids = InternationalDebtStatistics()
    df = ids.get_data(TDS_VARS, include_labels=True)
    df.to_parquet(Paths.raw_data / "ids_total_debt_service.parquet", index=False)

    logger.info("IDS total debt service data downloaded successfully.")


if __name__ == "__main__":
    get_debt_stocks_data()
    get_total_debt_service_data()
