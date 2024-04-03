"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

services_version = {
    "version": "0.4.0",
    "compileDate": "06/02/2024 08:37:11",
    "gitCommit": "ae87dbdef5a749cdbed66d7f0e841cc809ad2510",
}
services_list_metalakes = [
    {
        "name": "metalake_demo",
        "comment": "comment",
        "audit": {"creator": "anonymous", "createTime": "2024-03-30T13:49:53.382Z"},
    }
]
services_get_metalake = {
    "name": "metalake_demo",
    "comment": "comment",
    "audit": {"creator": "anonymous", "createTime": "2024-03-30T13:49:53.382Z"},
}
services_list_catalogs = [
    {"namespace": ["metalake_demo"], "name": "catalog_hive"},
    {"namespace": ["metalake_demo"], "name": "catalog_iceberg"},
    {"namespace": ["metalake_demo"], "name": "catalog_postgres"},
]
services_get_catalog = {"namespace": ["metalake_demo"], "name": "catalog_hive"}
services_list_schemas = [
    {"namespace": ["metalake_demo", "catalog_hive"], "name": "default"},
    {"namespace": ["metalake_demo", "catalog_hive"], "name": "sales"},
]
services_get_schema = {"namespace": ["metalake_demo", "catalog_hive"], "name": "sales"}
services_list_tables = [
    {"namespace": ["metalake_demo", "catalog_hive", "sales"], "name": "categories"},
    {"namespace": ["metalake_demo", "catalog_hive", "sales"], "name": "customers"},
    {"namespace": ["metalake_demo", "catalog_hive", "sales"], "name": "products"},
    {"namespace": ["metalake_demo", "catalog_hive", "sales"], "name": "sales"},
    {"namespace": ["metalake_demo", "catalog_hive", "sales"], "name": "stores"},
]
services_get_table = {
    "name": "sales",
    "comment": "",
    "columns": [
        {
            "name": "sale_id",
            "type": "integer",
            "nullable": True,
            "autoIncrement": False,
        },
        {
            "name": "employee_id",
            "type": "integer",
            "nullable": True,
            "autoIncrement": False,
        },
        {
            "name": "store_id",
            "type": "integer",
            "nullable": True,
            "autoIncrement": False,
        },
        {
            "name": "product_id",
            "type": "integer",
            "nullable": True,
            "autoIncrement": False,
        },
        {
            "name": "customer_id",
            "type": "integer",
            "nullable": True,
            "autoIncrement": False,
        },
        {"name": "sold", "type": "date", "nullable": True, "autoIncrement": False},
        {
            "name": "quantity",
            "type": "integer",
            "nullable": True,
            "autoIncrement": False,
        },
        {
            "name": "total_amount",
            "type": "decimal(10,2)",
            "nullable": True,
            "autoIncrement": False,
        },
    ],
    "properties": {
        "input-format": "org.apache.hadoop.mapred.TextInputFormat",
        "transient_lastDdlTime": "1711806631",
        "output-format": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "location": "hdfs://hive:9000/user/hive/warehouse/sales.db/sales",
        "table-type": "MANAGED_TABLE",
        "serde-lib": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "STATS_GENERATED_VIA_STATS_TASK": "workaround for potential lack of HIVE-12730",
        "serde-name": "sales",
    },
    "audit": {"creator": "anonymous", "createTime": "2024-03-30T13:50:31.289Z"},
    "distribution": {"strategy": "none", "number": 0, "funcArgs": []},
    "sortOrders": [],
    "partitioning": [],
    "indexes": [],
}
