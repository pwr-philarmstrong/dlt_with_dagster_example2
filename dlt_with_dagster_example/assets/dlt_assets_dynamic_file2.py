# Multiple dlt assets created with dynamically from public postgres database to filesystem with extra columns added and translation function
from collections.abc import Iterable
import yaml
import logging
import sqlalchemy as sa
import pyarrow as pa
import urllib.parse
import os
from datetime import datetime, timedelta
import time

from dagster import AssetExecutionContext, Definitions, AssetKey, ConfigurableResource, EnvVar
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets, DagsterDltTranslator
import dlt
from dlt.sources.sql_database import sql_database, sql_table
from dlt.common.pendulum import pendulum
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.destinations import filesystem
from dagster import AssetExecutionContext, StaticPartitionsDefinition
from dlt import pipeline
post_materialization_delay = 2 # seconds

from ..utils import to_snake_case

tables = ['dbo.clan', 'dbo.clan_membership',  'dbo.family']

output_dir = 'c:/data/dlt_example/rfam/'
destination = filesystem(bucket_url=f'file:///{output_dir}/')

database = 'Rfam'

connection_string="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"

dlt_resource = DagsterDltResource()

# Set the logging level for Azure SDK to WARNING
azure_logger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
azure_logger.setLevel(logging.WARNING)

# Set up logging with timestamps
logging.basicConfig(
    level=logging.INFO,  # Change to INFO to turn off DEBUG logging
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

dlt.config["normalize.parquet_normalizer.add_dlt_load_id"] = True

def query_adapter_callback(query, table):
    return query

def transform(doc, table_name):
    import pyarrow as pa
    from datetime import datetime, date

    table_name_column = pa.array([table_name] * len(doc))
    date_column = [datetime.now()] * len(doc)
    
    def get_year_month(dt):
        if isinstance(dt, datetime):
            return dt.year, dt.month
        elif isinstance(dt, date):
            return dt.year, dt.month
        else:
            raise ValueError("Unsupported date type")
    
    year_column = pa.array([get_year_month(dt)[0] for dt in date_column])
    month_column = pa.array([get_year_month(dt)[1] for dt in date_column])
    
    doc = doc.append_column("table_name", table_name_column)
    doc = doc.append_column("year", year_column)
    doc = doc.append_column("month", month_column) 
    
    return doc

def create_resource(table_name):
    print(f"Creating resource for my table: {table_name}")
    schema, table = table_name.split('.')
    unique_resource_name=f"{schema}_{table}"
            
    @dlt.resource(table_name=unique_resource_name)
    def resource():
        return sql_table(
            connection_string,
            backend="pyarrow",            
            table=table,
            #schema=schema,
            defer_table_reflect=True,                
            query_adapter_callback=query_adapter_callback
        ).add_map(lambda doc: transform(doc, f"{schema}.{table}")).parallelize()
    
    return resource()

class CustomDagsterDltTranslator(DagsterDltTranslator):
    def get_asset_key(self, resource: DagsterDltResource) -> AssetKey:
        """Overrides asset key to be the dlt resource name."""
        new_asset_name = to_snake_case(resource.name)
        return AssetKey(f"dlt_raw_assets_dynamic_file2__{new_asset_name}")

    def get_deps_asset_keys(self, resource: DagsterDltResource) -> Iterable[AssetKey]:
        """Overrides upstream asset key to be a single source asset."""
        return [AssetKey("common_upstream_dlt_dependency")]

@dlt.source
def raw_dynf2(table_name):
    my_source = create_resource(table_name)
    return my_source

def create_dlt_assets(tables):
    assets = []

    for table_name in tables:
        schema, table = table_name.split('.')

        unique_asset_name = f"dlt_assets_dynamic_file2__{table}"

        def create_asset_function(table_name, unique_asset_name):
            @dlt_assets(
                dlt_source=raw_dynf2(
                    table_name
                ),
                dlt_pipeline=pipeline(        
                    pipeline_name=f"{unique_asset_name}_pipeline",
                    destination=destination,
                    dataset_name="dlt_assets_dynamic_file2",
                    progress="log",
                ),
                name=unique_asset_name, 
                group_name="dlt_assets_dynamic_file2",
                dagster_dlt_translator=CustomDagsterDltTranslator(),                    
            )
            def dagster_sql_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
                yield from dlt.run(context=context, 
                    write_disposition="append",       
                )
                time.sleep(post_materialization_delay)
            
            return dagster_sql_assets

        asset_function = create_asset_function(table_name, unique_asset_name)      

        assets.append(asset_function)
    
    return assets


# Create Dagster SQL assets
dagster_sql_assets = create_dlt_assets(tables)