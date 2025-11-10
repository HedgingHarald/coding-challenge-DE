import pandas as pd
import logging
from typing import Dict
import json

from coding_challenge.utils.transformations import (
    handle_empty_numeric,
    calculate_returns_from_sales,
    aggregate_multiple_deliveries,
    deduplicate_incremental_data,
    calculate_stockout_simple,
)

logger = logging.getLogger(__name__)


def process_deliveries_sales_json(json_dict: Dict[str, str]) -> pd.DataFrame:
    all_records = []
    
    for filename, json_content_or_func in json_dict.items():
        json_content = json_content_or_func() if callable(json_content_or_func) else json_content_or_func
        
        if isinstance(json_content, str):
            data = json.loads(json_content)
        else:
            data = json_content
        
        parts = filename.replace('.json', '').split('_')
        extraction_date = pd.to_datetime(f"{parts[-6]}-{parts[-5]}-{parts[-4]}")
        
        filialen = data[0]["Filiale"] if isinstance(data, list) else data["Filiale"]
        
        for filiale_entry in filialen:
            datum_str = filiale_entry["Datum"]
            filial_nummer = int(filiale_entry["FilialNummer"])
            target_date = pd.to_datetime(datum_str, format="%d/%m/%y")
            
            for artikel in filiale_entry["ArtikelHistory"]:
                record = {
                    'target_date': target_date,
                    'number_store': filial_nummer,
                    'number_product': int(artikel["ArtikelNummer"]),
                    'delivery_qty': float(artikel["Liefermenge"]),
                    'sales_qty': float(artikel["Verkaufsmenge"]),
                    'extraction_date': extraction_date
                }
                all_records.append(record)
    
    combined = pd.DataFrame(all_records)
    
    combined = deduplicate_incremental_data(
        combined,
        key_cols=['target_date', 'number_store', 'number_product']
    )
    
    aggregated = aggregate_multiple_deliveries(
        combined,
        group_cols=['target_date', 'number_store', 'number_product'],
        agg_col='delivery_qty'
    )
    
    aggregated = aggregated.merge(
        combined.groupby(['target_date', 'number_store', 'number_product'])['sales_qty'].first(),
        on=['target_date', 'number_store', 'number_product']
    )
    
    aggregated['return_qty'] = 0.0
    
    return aggregated[['target_date', 'number_store', 'number_product', 'sales_qty', 'return_qty', 'delivery_qty']]


def process_products_json(json_dict: Dict[str, str]) -> pd.DataFrame:
    all_products = []
    
    for filename, json_content_or_func in json_dict.items():
        json_content = json_content_or_func() if callable(json_content_or_func) else json_content_or_func
        
        if isinstance(json_content, str):
            data = json.loads(json_content)
        else:
            data = json_content
        
        parts = filename.replace('.json', '').split('_')
        extraction_date = pd.to_datetime(f"{parts[-6]}-{parts[-5]}-{parts[-4]}")
        
        products = data["Artikel"] if "Artikel" in data else data
        
        for product in products:
            moq_value = product.get("BestellMindestEinheit", 0)
            if isinstance(moq_value, str):
                moq_value = int(float(moq_value)) if moq_value else 0
            
            record = {
                'number_product': int(product["ArtikelNummer"]),
                'product_name': product["ArtikelName"],
                'moq': int(moq_value),
                'extraction_date': extraction_date
            }
            all_products.append(record)
    
    combined = pd.DataFrame(all_products)
    
    combined = combined.sort_values('extraction_date').drop_duplicates(
        subset=['number_product'],
        keep='last'
    )
    
    return combined[['number_product', 'product_name', 'moq']]


def process_prices_json(json_dict: Dict[str, str]) -> pd.DataFrame:
    all_prices = []
    
    for filename, json_content_or_func in json_dict.items():
        json_content = json_content_or_func() if callable(json_content_or_func) else json_content_or_func
        
        if isinstance(json_content, str):
            data = json.loads(json_content)
        else:
            data = json_content
        
        prices = data["Verkaufspreise"] if "Verkaufspreise" in data else data
        
        for price_entry in prices:
            record = {
                'number_product': int(price_entry["ArtikelNummer"]),
                'price': float(price_entry.get("ArtikelPreis", price_entry.get("Artikelpreis", 0)))
            }
            all_prices.append(record)
    
    combined = pd.DataFrame(all_prices)
    combined = combined.drop_duplicates(subset=['number_product'], keep='last')
    
    return combined


def process_stores_json(json_dict: Dict[str, str]) -> pd.DataFrame:
    all_stores = []
    
    for filename, json_content_or_func in json_dict.items():
        json_content = json_content_or_func() if callable(json_content_or_func) else json_content_or_func
        
        if isinstance(json_content, str):
            data = json.loads(json_content)
        else:
            data = json_content
        
        stores = data["Filialliste"] if "Filialliste" in data else data
        
        for store in stores:
            record = {
                'number_store': int(store["FilialNummer"]),
                'store_name': store["FilialName"],
                'store_address': store["FilialAnschrift"].replace('\n', ' – ')
            }
            all_stores.append(record)
    
    combined = pd.DataFrame(all_stores)
    combined = combined.drop_duplicates(subset=['number_store'], keep='last')
    
    return combined


def join_galaxy_data(
    sales_deliveries: pd.DataFrame,
    products: pd.DataFrame,
    prices: pd.DataFrame,
    stores: pd.DataFrame,
    mapping_product: pd.DataFrame,
    mapping_store: pd.DataFrame
) -> pd.DataFrame:
    merged = sales_deliveries.copy()
    
    merged = merged.merge(mapping_product, on='number_product', how='left')
    merged = merged.merge(mapping_store, on='number_store', how='left')
    merged = merged.merge(products, on='number_product', how='left')
    merged = merged.merge(prices, on='number_product', how='left')
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
