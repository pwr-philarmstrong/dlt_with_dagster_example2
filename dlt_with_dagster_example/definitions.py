from dagster import Definitions, load_assets_from_modules
from dagster_embedded_elt.dlt import DagsterDltResource
from .assets import (
    dlt_assets,
    dlt_assets_mssql_incremental2, 
    dlt_assets_multi, 
    dlt_assets_with_partition, 
    dlt_assets_with_partition_file, 
    dlt_assets_incremental, 
    #dlt_assets_dynamic,
    dlt_assets_dynamic_file, 
    dlt_assets_dynamic_file2,
    dlt_assets_dynamic_mssql_file3, 
)

import psutil
import os
import logging

def get_memory_usage():
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024  # Convert to MB
    logging.info(f"Current memory usage: {memory_mb:.2f} MB")
    return memory_mb

dlt_assets = load_assets_from_modules([dlt_assets])
dlt_assets_multi = load_assets_from_modules([dlt_assets_multi])
dlt_assets_with_partition = load_assets_from_modules([dlt_assets_with_partition])
dlt_assets_with_partition_file = load_assets_from_modules([dlt_assets_with_partition_file])
dlt_assets_incremental = load_assets_from_modules([dlt_assets_incremental])
dlt_assets_mssql_incremental2 = load_assets_from_modules([dlt_assets_mssql_incremental2])
#dlt_assets_dynamic = load_assets_from_modules([dlt_assets_dynamic])
dlt_assets_dynamic_file = load_assets_from_modules([dlt_assets_dynamic_file])
dlt_assets_dynamic_file2 = load_assets_from_modules([dlt_assets_dynamic_file2])
dlt_assets_dynamic_mssql_file3 = load_assets_from_modules([dlt_assets_dynamic_mssql_file3])
#dlt_assets_dynamic = load_assets_from_modules([dlt_assets_dynamic])

# Ensure all variables are iterables before unpacking
assets = [
    *dlt_assets,
    *dlt_assets_multi,
    *dlt_assets_with_partition,
    *dlt_assets_with_partition_file,
    *dlt_assets_incremental,
    #*dlt_assets_mssql_incremental2,
    #*dlt_assets_dynamic,
    *dlt_assets_dynamic_file,
    *dlt_assets_dynamic_file2,
    #*dlt_assets_dynamic_mssql_file3,    
    #*dlt_assets_dynamic,
]

defs = Definitions(
    assets=assets,
    resources={
        "dlt": DagsterDltResource(),
    },
)

# Track final memory
get_memory_usage()
