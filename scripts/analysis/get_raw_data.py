"""Get raw data"""

import signal
from collections.abc import Callable
from typing import Any

from bblocks.data_importers import InternationalDebtStatistics

from scripts.config import Paths
from scripts.logger import logger


def timeout_30min(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to timeout a function after 30 minutes and implement a
    try except block to catch any exceptions raised within the function."""

    def handler(signum: int, frame: Any) -> None:
        raise TimeoutError("Function timed out after 30 minutes")

    def wrapper() -> Any:
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(30 * 60)  # 30 minutes
        try:
            return func()
        except Exception as e:
            raise RuntimeError(f"Could not complete data download: {e!s}") from e
        finally:
            signal.alarm(0)

    return wrapper


@timeout_30min
def get_debt_stocks_data() -> None:
    """Get the raw data for the International Debt Statistics."""

    ids = InternationalDebtStatistics()
    inds = list(ids.debt_stock_indicators.indicator_code.unique())

    df = ids.get_data(inds, include_labels=True)
    df.to_parquet(Paths.raw_data / "ids_debt_stocks.parquet", index=False)

    logger.info("IDS debt stocks data downloaded successfully.")


if __name__ == "__main__":
    get_debt_stocks_data()
