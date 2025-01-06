# Single dlt asset created with manual function calls from public postgres database with fake partitions to filesystem
import yaml
import logging
import sqlalchemy as sa
import pyarrow as pa
import urllib.parse
import os

from dagster import AssetExecutionContext, Definitions
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets
import dlt
from dlt.sources.sql_database import sql_database, sql_table
from dlt.common.pendulum import pendulum
from dlt.sources.credentials import ConnectionStringCredentials
from dlt.destinations import filesystem
from dagster import AssetExecutionContext, StaticPartitionsDefinition
from dlt import pipeline #, resource , sql_table, transform

from ..partitions import my_partitions

table_name='family'

output_dir = 'c:/data/dlt_example/rfam/'
connection_string="mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
destination = filesystem(bucket_url=f'file:///{output_dir}/')
#destination = duckdb

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
    return doc     

def create_resource(table_name):
    print(f"Creating resource for my table: {table_name}")
            
    @dlt.resource(table_name=f"{table_name}")
    def resource():
        return sql_table(
            backend="pyarrow",            
            credentials=ConnectionStringCredentials(connection_string),
            table=table_name,
            defer_table_reflect=True,                
            query_adapter_callback=query_adapter_callback
        ).parallelize()
    
    return resource()

@dlt.source
def rawdatapart2(table_name):
    my_source = create_resource(table_name)
    return my_source

@dlt_assets(
    dlt_source=rawdatapart2(
        table_name
    ),
    dlt_pipeline=pipeline(
        pipeline_name="dlt_assets_with_partition_file_pipeline",
        destination=destination,
        dataset_name="dlt_assets_with_partition_file",
        progress="log",
    ),
    partitions_def=my_partitions,
    name=f"partitioned_table_file_{table_name}", 
    group_name="dlt_assets_with_partition_file",
)
def dagster_sql_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    yield from dlt.run(context=context, 
        write_disposition="append",       
    )
