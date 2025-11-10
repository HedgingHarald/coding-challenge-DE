
from kedro.pipeline import Pipeline, node, pipeline

from .nodes import (
    process_sales_files,
    process_delivery_files,
    process_product_master,
    process_store_master,
    join_all_data,
    create_ml_dataset,
    create_app_dataset,
)


def create_pipeline(**kwargs) -> Pipeline:

    return pipeline(
        [
            # Node 1: Process sales files
            node(
                func=process_sales_files,
                inputs="raw_sales",
                outputs="intermediate_sales",
                name="process_sales",
            ),
            
            # Node 2: Process delivery files
            node(
                func=process_delivery_files,
                inputs="raw_deliveries",
                outputs="intermediate_deliveries",
                name="process_deliveries",
            ),
            
            # Node 3: Process product master
            node(
                func=process_product_master,
                inputs="raw_products",
                outputs="intermediate_products",
                name="process_products",
            ),
            
            # Node 4: Process store master
            node(
                func=process_store_master,
                inputs="raw_stores",
                outputs="intermediate_stores",
                name="process_stores",
            ),
            
            # Node 5: Join all data
            node(
                func=join_all_data,
                inputs=[
                    "intermediate_sales",
                    "intermediate_deliveries",
                    "intermediate_products",
                    "intermediate_stores",
                    "mapping_product",
                    "mapping_store",
                ],
                outputs="primary_dataset",
                name="join_all_data",
            ),
            
            # Node 6: Create ML dataset
            node(
                func=create_ml_dataset,
                inputs="primary_dataset",
                outputs="ml_dataset",
                name="create_ml_dataset",
            ),
            
            # Node 7: Create App dataset
            node(
                func=create_app_dataset,
                inputs="primary_dataset",
                outputs="app_dataset",
                name="create_app_dataset",
            ),
        ]
    )

