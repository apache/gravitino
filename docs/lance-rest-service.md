---
title: "Lance REST service"
slug: /lance-rest-service
keywords:
  - Lance REST 
license: "This software is licensed under the Apache License version 2."
---


## Background

Since version 1.1.0, Gravitino includes a REST service for Lance datasets. The Lance REST service is a web service that allows you to interact with Lance datasets over HTTP. It provides endpoints for querying, inserting, updating, and deleting data in Lance datasets.
It abides by the [Lance REST API specification](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/lancedb/lance-namespace/refs/heads/main/docs/src/rest.yaml). More details about the specification, please refer to docs [here](https://lance.org/format/namespace/impls/rest/)

In 1.1.0, Lance REST service will be run within Gravitino server and standalone Lance REST service will be supported in future releases if needed.


## Capabilities

The Lance REST service supports the APIs defined in the Lance REST API specification. The following are some of the key capabilities of the Lance REST service:
- Namespace management including creating namespace, listing namespaces, describing, deleting namespace, namespace exists check.
- Table management including creating tables including creating empty tables, dropping tables, registering tables and unregistering tables.
- Index management including creating index, listing indexes. Dropping index is not supported in 1.1.0.

Full Supports are listed as the following table:

| Operation ID         | Description                                                                                         | Since version | 
|----------------------|-----------------------------------------------------------------------------------------------------|---------------|
| CreateNamespace      | Create a Lance namespace                                                                            | 1.1.0         |              
| ListNamespaces       | List all namespaces under a specific namespace                                                      | 1.1.0         |
| DescribeNamespace    | Get details of a specific namespace                                                                 | 1.1.0         |                             
| DropNamespace        | Delete a specific namespace                                                                         | 1.1.0         |                            
| NamespaceExists      | Check if a namespace exists                                                                         | 1.1.0         |                             
| ListTables           | List all tables in a specific namespace                                                             | 1.1.0         | 
| CreateTable          | Create a new table in a specific namespace                                                          | 1.1.0         |
| DropTable            | Delete a specific table from a namespace, drop table will drop metadata and Lance data all together | 1.1.0         |
| TableExists          | Check if a specific table exists in a namespace                                                     | 1.1.0         |
| RegisterTable        | Register an existing Lance table to a specific namespace                                            | 1.1.0         |
| deregisterTable      | Unregister a specific table from a namespace, it will only remove metadata, Lance data will be kept | 1.1.0         |   
| CreateIndex          | Create an index on a specific table                                                                 | 1.1.0         |
| ListIndexes          | List all indexes on a specific table                                                                | 1.1.0         |

## Getting started
To use the Lance REST service, you need to have Gravitino server running with Lance REST service enabled. The following are configurations to enable Lance REST service in Gravitino server.

| Configuration item                             | Description                                                                                                                                                                                                                         | Default value           | Required                                   | Since Version |
|------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|--------------------------------------------|---------------|
| `gravitino.auxService.names`                   | Auxiliary service that runs Lance REST service, currently it supports `iceberg-rest` and `lance-rest`. It should include `lance-rest` if you want to start the Lance REST service like `lance-rest`, or `lance-rest, iceberg-rest`  | iceberg-rest,lance-rest | Yes if Lance REST service is going to run  | 0.2.0         |
| `gravitino.lance-rest.classpath`               | The class path of lance-rest service, it's the relative path compared with Gravitino home.                                                                                                                                          | lance-rest-server/libs  | Yes if Lance REST service is going to run  | 1.1.0         |
| `gravitino.lance-rest.port`                    | The port number that Lance REST service listens on.                                                                                                                                                                                 | 9101                    | Yes if Lance REST service is going to run  | 1.1.0         |
| `gravitino.lance-rest.host`                    | The host name that Lance REST service run in                                                                                                                                                                                        | 0.0.0.0                 | Yes if Lance REST service is going to run  | 1.1.0         | 
| `gravitino.lance-rest.namespace-backend`       | backend to store namespace metadata, currently it only supports `gravitino`                                                                                                                                                         | gravitino               | Yes if Lance REST service is going to run  | 1.1.0         |
| `gravitino.lance-rest.gravitino.uri`           | Gravitino server URI, it should be set when `namespace-backend` is `gravitino`                                                                                                                                                      | http://localhost:8090   | Yes if Lance REST service is going to run  | 1.1.0         |
| `gravitino.lance-rest.gravitino.metalake-name` | Gravitino metalake name, it should be set when `namespace-backend` is `gravitino`                                                                                                                                                   | (none)                  | Yes if Lance REST service is going to run  | 1.1.0         |


## Example

When Gravitino server is started with Lance REST service starts successfully, you can use the following example to create a namespace and a table in the namespace. 

```shell
pip install lance-namespace
```
Then, 

```python

import lance_namespace as ln
ns = ln.connect("rest", {"uri": "http://localhost:9101/lance"})
# Create a namespace
create_catalog_ns_request = ln.CreateNamespaceRequest(id=["lance_catalog"])
catalog = ns.create_namespace(create_catalog_ns_request)
create_schema_ns_request = ln.CreateNamespaceRequest(id=["lance_catalog", "schema"])
schema = ns.create_namespace(create_schema_ns_request)

# register a table
register_table_request=ln.RegisterTableRequest(id=['lance_catalog','schema','table01'], location='/tmp/lance_catalog/schema/table01')
ns.register_table(register_table_request)
...
```

Other languages can also interact with Lance REST service by sending HTTP requests to the service endpoints defined in the [Lance REST API specification](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/lancedb/lance-namespace/refs/heads/main/docs/src/rest.yaml).
