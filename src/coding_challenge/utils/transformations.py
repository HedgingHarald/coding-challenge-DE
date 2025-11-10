import pandas as pd
import numpy as np


def handle_empty_numeric(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(
        series.replace('', np.nan),
        errors='coerce'
    ).fillna(default)


def calculate_returns_from_sales(sales_qty: pd.Series) -> pd.DataFrame:
    return pd.DataFrame({
        'sales_qty': sales_qty.clip(lower=0),
        'return_qty': sales_qty.clip(upper=0).abs()
    })


def aggregate_multiple_deliveries(
    df: pd.DataFrame,
    group_cols: list,
    agg_col: str = 'delivery_qty'
) -> pd.DataFrame:
    return df.groupby(group_cols, as_index=False).agg({agg_col: 'sum'})


def deduplicate_incremental_data(
    df: pd.DataFrame,
    key_cols: list,
    extraction_date_col: str = 'extraction_date'
) -> pd.DataFrame:
    return df.sort_values(extraction_date_col).drop_duplicates(
        subset=key_cols,
        keep='last'
    )


def calculate_stockout_simple(
    sales_qty: pd.Series,
    delivery_qty: pd.Series
) -> pd.Series:
    return sales_qty > delivery_qty
