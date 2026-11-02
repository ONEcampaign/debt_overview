"""Utility functions"""

import signal
from collections.abc import Callable
from functools import cache
from typing import Any

import pandas as pd
import resolvekit as rk
from bblocks.data_importers import WEO
from pydeflate import imf_exchange, set_pydeflate_path

from scripts.config import Paths

set_pydeflate_path(Paths.raw_data)

# Country names (IMF spellings, from the DSA list) that resolvekit doesn't match
_NAME_CROSSWALK = rk.Crosswalk.from_dict(
    {
        "Lao P.D.R.": "country/LAO",
        "Somalia, Fed. Rep.": "country/SOM",
    }
)


@cache
def _country_resolver() -> rk.Resolver:
    """Resolver restricted to countries, so country names never match a
    same-named region (e.g. "Micronesia", "Kosovo")."""
    return rk.Resolver.from_modules(module_ids=["geo.countries"])


def names_to_iso3(names: pd.Series, *, strict: bool = False) -> pd.Series:
    """Resolve country names to ISO3 codes, offline.

    Args:
        names: Series of country names.
        strict: If True, raise when a name can't be resolved to a single
            country. If False, unresolved names (e.g. aggregates such as
            "Low & middle income") become None.

    Returns:
        Series of ISO3 codes aligned to the input index.
    """
    policy = "raise" if strict else "null"
    return _country_resolver().bulk(
        values=names,
        to="iso3",
        on_missing="null",
        not_found=policy,
        on_ambiguous=policy,
        crosswalk=_NAME_CROSSWALK,
    )


@cache
def african_iso3_codes() -> frozenset[str]:
    """ISO3 codes of countries in Africa (UN M.49)."""
    codes = rk.default().within("Africa", entity_type="geo.country", to="iso3")
    return frozenset(c for c in codes if c)


@cache
def high_income_iso3_codes() -> frozenset[str]:
    """ISO3 codes of countries in the World Bank high income group."""
    return frozenset(
        rk.default().members_of("World Bank High-Income Countries", as_codes="iso3")
    )


def custom_sort(
    df: pd.DataFrame, resort_dict: dict[str, list[str] | str]
) -> pd.DataFrame:
    """Custom sort columns placing specific items on top and sorting the
    rest alphabetically

    Args:
        df: DataFrame to resort
        resort_dict: Dictionary of columns and items to sort at the top of the column. The dictionary
           should be in the format {col name: items to sort at top}.

    Returns:
        A resorted DataFrame
    """

    # create a copy of df

    _df = df.copy(deep=True)

    # convert all values to list
    for k, v in resort_dict.items():
        if isinstance(v, str):
            resort_dict[k] = [resort_dict[k]]

        # check if all passed columns exist
        if k not in list(_df.columns):
            raise ValueError(f"Column not found: {k}")

    for col, values in resort_dict.items():
        _df[col] = pd.Categorical(
            _df[col],
            categories=values
            + sorted(val for val in _df[col].unique() if val not in values),
            ordered=True,
        )

    _df = _df.sort_values(list(resort_dict.keys()))
    return _df


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


def add_africa_values(df, agg_operation: "sum") -> pd.DataFrame:
    """Add Africa (excluding high income) aggregate values to a dataframe.

    Args:
        df: DataFrame containing country level data with columns 'entity_name' and 'value'.
        agg_operation: Aggregation operation to use when calculating the Africa values. Default is 'sum

    Returns:
        DataFrame with Africa (excluding high income) aggregate values added.
    """

    dff = df.copy(deep=True)  # make a copy to avoid modifying the original dataframe

    afr_dff = (
        df.assign(iso3_code=lambda d: names_to_iso3(d.entity_name))
        .dropna(subset=["iso3_code"])
        .loc[
            lambda d: (
                d.iso3_code.isin(african_iso3_codes())
                & ~d.iso3_code.isin(high_income_iso3_codes())
            )
        ]
        .drop(columns=["iso3_code"])
        .dropna(subset="value")
        .groupby(
            [
                i
                for i in dff.columns
                if i not in ["value", "entity_name", "entity_code"]
            ],
            observed=True,
        )
        .agg({"value": agg_operation})
        .reset_index()
        .assign(entity_name="Africa (excluding high income)", is_aggregate=True)
    )

    dff = pd.concat([dff, afr_dff], ignore_index=True)

    return dff


def format_values(value: float | int) -> str:
    """Format a numeric value into a human-readable string with appropriate
    units (millions, billions, trillions).

    Args:
        value: Numeric value to format.

    Returns:
        Formatted string representation of the value.
    """
    if value < 1_000_000:
        return f"US${value:,.0f}"

    elif value < 1_000_000_000:
        scaled = value / 1_000_000
        fmt = f"{scaled:.1f}" if not scaled.is_integer() else f"{int(scaled)}"
        return f"US${fmt} million"

    elif value < 1_000_000_000_000:
        scaled = value / 1_000_000_000
        fmt = f"{scaled:.1f}" if not scaled.is_integer() else f"{int(scaled)}"
        return f"US${fmt} billion"

    else:
        scaled = value / 1_000_000_000_000
        fmt = f"{scaled:.1f}" if not scaled.is_integer() else f"{int(scaled)}"
        return f"US${fmt} trillion"


def get_gov_expenditure_curr_usd() -> pd.DataFrame:
    """ """

    weo = WEO()
    return (
        weo.get_data()
        .loc[lambda d: d.indicator_code == "GGX"]
        .assign(value=lambda d: d.value * d.scale_code)
        .loc[:, ["entity_code", "year", "value"]]
        .pipe(
            imf_exchange,
            source_currency="LCU",
            target_currency="USD",
            id_column="entity_code",
            value_column="value",
            target_value_column="value",
        )
        .dropna(subset=["value"])
    )
