# Multiple dlt assets created with dynamically from public postgres database to filesystem with extra columns added
import yaml
import logging
import sqlalchemy as sa
import pyarrow as pa
import urllib.parse
import os
import pyarrow as pa
from datetime import datetime, date

from dagster import AssetExecutionContext, Definitions
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
import dlt
from dlt.sources.sql_database import sql_database, sql_table
from dlt.common.pendulum import pendulum
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.destinations import filesystem
from dagster import StaticPartitionsDefinition
from dlt import pipeline

tables = ['clan_membership', 'clan', 'family']

dlt_resource = DagsterDltResource()

output_dir = 'c:/data/dlt_example/rfam/'
connection_string="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
destination = filesystem(bucket_url=f'file:///{output_dir}/')
#destination = duckdb

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
    logger.info(f"Creating resource for my table: {table_name}")
            
    @dlt.resource(table_name=f"{table_name}")
    def resource():
        return sql_table(
            backend="pyarrow",            
            credentials=ConnectionStringCredentials(connection_string),
            table=table_name,
            defer_table_reflect=True,                
            query_adapter_callback=query_adapter_callback
        ).add_map(lambda doc: transform(doc, f"{table_name}")).add_limit(10000)#.parallelize()
    
    return resource()

@dlt.source
def raw_dynf(table_name):
    my_source = create_resource(table_name)
    return my_source

def create_dlt_assets(tables):
    assets = []

    for table in tables:
        table_name = table

        unique_asset_name = f"dlt_assets_dynamic_file__{table_name}"

        def create_asset_function(table_name, unique_asset_name):
            @dlt_assets(
                dlt_source=raw_dynf(
                    table_name
                ),
                dlt_pipeline=pipeline(        
                    pipeline_name=f"{unique_asset_name}_pipeline",
                    destination=destination,
                    dataset_name="dlt_assets_dynamic_file",
                    progress="log",
                ),
                name=unique_asset_name, 
                group_name="dlt_assets_dynamic_file",
            )
            def dagster_sql_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
                yield from dlt.run(context=context, 
                    write_disposition="append",       
                )
            
            return dagster_sql_assets

        asset_function = create_asset_function(table_name, unique_asset_name)      

        assets.append(asset_function)
    
    return assets

# Create Dagster SQL assets
dagster_sql_assets = create_dlt_assets(tables)