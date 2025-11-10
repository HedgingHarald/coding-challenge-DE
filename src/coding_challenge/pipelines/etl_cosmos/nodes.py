import pandas as pd
import logging
from typing import Dict

from coding_challenge.utils.transformations import (
    handle_empty_numeric,
    calculate_returns_from_sales,
    aggregate_multiple_deliveries,
    deduplicate_incremental_data,
    calculate_stockout_simple,
)

logger = logging.getLogger(__name__)


def process_sales_files(sales_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    all_sales = []
    
    for filename, df_or_func in sales_dict.items():
        df = df_or_func() if callable(df_or_func) else df_or_func
        
        parts = filename.replace('.csv', '').split('_')
        extraction_date = pd.to_datetime(f"{parts[-3]}-{parts[-2]}-{parts[-1]}")
        
        df = df.copy()
        df['extraction_date'] = extraction_date
        all_sales.append(df)
    
    combined = pd.concat(all_sales, ignore_index=True)
    
    combined = combined.rename(columns={
        'Datum': 'target_date',
        'Kunde': 'number_store',
        'Artikel': 'number_product',
        'VK-Menge': 'raw_quantity',
        'VK-Betrag': 'revenue'
    })
    
    combined['target_date'] = pd.to_datetime(combined['target_date'])
    
    combined = deduplicate_incremental_data(
        combined,
        key_cols=['target_date', 'number_store', 'number_product']
    )
    
    combined['raw_quantity'] = handle_empty_numeric(combined['raw_quantity'])
    
    split_data = calculate_returns_from_sales(combined['raw_quantity'])
    combined['sales_qty'] = split_data['sales_qty']
    combined['return_qty'] = split_data['return_qty']
    
    return combined[['target_date', 'number_store', 'number_product', 'sales_qty', 'return_qty']]


def process_delivery_files(delivery_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    all_deliveries = []
    
    for filename, df_or_func in delivery_dict.items():
        df = df_or_func() if callable(df_or_func) else df_or_func
        
        parts = filename.replace('.csv', '').split('_')
        extraction_date = pd.to_datetime(f"{parts[-3]}-{parts[-2]}-{parts[-1]}")
        
        df = df.copy()
        df['extraction_date'] = extraction_date
        all_deliveries.append(df)
    
    combined = pd.concat(all_deliveries, ignore_index=True)
    
    combined = combined.rename(columns={
        'Datum': 'target_date',
        'ArtNr': 'number_product',
        'Kunde_Nummer': 'number_store',
        'LI-Menge': 'delivery_qty'
    })
    
    combined['target_date'] = pd.to_datetime(combined['target_date'])
    combined['delivery_qty'] = handle_empty_numeric(combined['delivery_qty'])
    
    combined = deduplicate_incremental_data(
        combined,
        key_cols=['target_date', 'number_store', 'number_product']
    )
    
    aggregated = aggregate_multiple_deliveries(
        combined,
        group_cols=['target_date', 'number_store', 'number_product'],
        agg_col='delivery_qty'
    )
    
    return aggregated


def process_product_master(product_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    all_products = []
    
    for filename, df_or_func in product_dict.items():
        df = df_or_func() if callable(df_or_func) else df_or_func
        
        parts = filename.split('_')
        date_part = f"{parts[1]}-{parts[2]}-{parts[3]}"
        extraction_date = pd.to_datetime(date_part)
        
        df = df.copy()
        df['extraction_date'] = extraction_date
        all_products.append(df)
    
    combined = pd.concat(all_products, ignore_index=True)
    
    combined = combined.rename(columns={
        'ArtNr': 'number_product',
        'Bezeichnung': 'product_name',
        'Preis': 'price',
        'Mindestbestellmenge': 'moq'
    })
    
    combined = combined.sort_values('extraction_date').drop_duplicates(
        subset=['number_product'],
        keep='last'
    )
    
    combined['price'] = handle_empty_numeric(combined['price'])
    combined['moq'] = handle_empty_numeric(combined['moq'], default=0)
    
    return combined[['number_product', 'product_name', 'price', 'moq']]


def process_store_master(store_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    all_stores = []
    
    for filename, df_or_func in store_dict.items():
        df = df_or_func() if callable(df_or_func) else df_or_func
        
        parts = filename.split('_')
        date_part = f"{parts[1]}-{parts[2]}-{parts[3]}"
        extraction_date = pd.to_datetime(date_part)
        
        df = df.copy()
        df['extraction_date'] = extraction_date
        all_stores.append(df)
    
    combined = pd.concat(all_stores, ignore_index=True)
    
    combined = combined.rename(columns={
        'Nummer': 'number_store',
        'Straße': 'store_name',
        'PLZ': 'postal_code',
        'Ort': 'city'
    })
    
    combined = combined.sort_values('extraction_date').drop_duplicates(
        subset=['number_store'],
        keep='last'
    )
    
    combined['store_address'] = (
        combined['store_name'] + ' – ' +
        combined['postal_code'].astype(str) + ' – ' +
        combined['city']
    )
    
    return combined[['number_store', 'store_name', 'store_address']]


def join_all_data(
    sales: pd.DataFrame,
    deliveries: pd.DataFrame,
    products: pd.DataFrame,
    stores: pd.DataFrame,
    mapping_product: pd.DataFrame,
    mapping_store: pd.DataFrame
) -> pd.DataFrame:
    merged = pd.merge(
        sales,
        deliveries,
        on=['target_date', 'number_store', 'number_product'],
        how='outer'
    ).fillna({'sales_qty': 0, 'return_qty': 0, 'delivery_qty': 0})
    
    merged = merged.merge(mapping_product, on='number_product', how='left')
    merged = merged.merge(mapping_store, on='number_store', how='left')
    
    merged = merged.merge(products, on='number_product', how='left')
    merged = merged.merge(stores, on='number_store', how='left')
    
    merged['stockout'] = calculate_stockout_simple(
        merged['sales_qty'],
        merged['delivery_qty']
    )
    
    return merged


def create_ml_dataset(df: pd.DataFrame) -> pd.DataFrame:
    return df[[
        'id_product',
        'id_store',
        'target_date',
        'sales_qty',
        'stockout'
    ]].copy()


def create_app_dataset(df: pd.DataFrame) -> pd.DataFrame:
    return df[[
        'id_product',
        'id_store',
        'target_date',
        'sales_qty',
        'return_qty',
        'delivery_qty',
        'stockout',
        'price',
        'product_name',
        'number_product',
        'moq',
        'number_store',
        'store_name',
        'store_address'
    ]].copy()
