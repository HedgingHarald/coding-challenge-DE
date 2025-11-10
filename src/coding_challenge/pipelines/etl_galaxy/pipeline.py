
from kedro.pipeline import Pipeline, node
from .nodes import (
    process_deliveries_sales_json,
    process_products_json,
    process_prices_json,
    process_stores_json,
    join_galaxy_data,
    create_ml_dataset,
    create_app_dataset,
)


def create_pipeline(**kwargs) -> Pipeline:

    return Pipeline(
        [
            # Node 1: Process deliveries & sales JSON (nested structure)
            node(
                func=process_deliveries_sales_json,
                inputs="raw_deliveries_sales",
                outputs="galaxy_intermediate_sales_deliveries",
                name="process_deliveries_sales_json",
            ),
            # Node 2: Process product master JSON
            node(
                func=process_products_json,
                inputs="raw_products",
                outputs="galaxy_intermediate_products",
                name="process_products_json",
            ),
            # Node 3: Process price JSON
            node(
                func=process_prices_json,
                inputs="raw_prices",
                outputs="galaxy_intermediate_prices",
                name="process_prices_json",
            ),
            # Node 4: Process store master JSON
            node(
                func=process_stores_json,
                inputs="raw_stores",
                outputs="galaxy_intermediate_stores",
                name="process_stores_json",
            ),
            # Node 5: Join all data sources + apply ID mappings
            node(
                func=join_galaxy_data,
                inputs=[
                    "galaxy_intermediate_sales_deliveries",
                    "galaxy_intermediate_products",
                    "galaxy_intermediate_prices",
                    "galaxy_intermediate_stores",
                    "mapping_product",
                    "mapping_store",
                ],
                outputs="galaxy_primary_dataset",
                name="join_galaxy_data",
            ),
            # Node 6: Create ML dataset (minimal fields)
            node(
                func=create_ml_dataset,
                inputs="galaxy_primary_dataset",
                outputs="galaxy_ml_dataset",
                name="galaxy_create_ml_dataset",
            ),
            # Node 7: Create App dataset (all fields)
            node(
                func=create_app_dataset,
                inputs="galaxy_primary_dataset",
                outputs="galaxy_app_dataset",
                name="galaxy_create_app_dataset",
            ),
        ]
    )

