"""Utility functions"""

import pandas as pd


def custom_sort(df: pd.DataFrame, resort_dict: dict[str, list[str] | str]) -> pd.DataFrame:
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
    for k,v in resort_dict.items():
        if isinstance(v, str):
            resort_dict[k] = [resort_dict[k]]

        # check if all passed columns exist
        if k not in list(_df.columns):
            raise ValueError(f"Column not found: {k}")


    for col, values in resort_dict.items():
        _df[col] = pd.Categorical(
                _df[col],
                categories=values + sorted(val for val in _df[col].unique() if val not in values),
                ordered=True,
            )

    _df = _df.sort_values(list(resort_dict.keys()))
    return _df






