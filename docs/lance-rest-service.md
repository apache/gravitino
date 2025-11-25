---
title: "Lance REST service"
slug: /lance-rest-service
keywords:
  - Lance REST 
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Background

Since version 1.1.0, Gravitino includes a REST service for Lance datasets. The Lance REST service is a web service that allows you to interact with Lance datasets over HTTP. It provides endpoints for querying, inserting, updating, and deleting data in Lance datasets.
It abides by the [Lance REST API specification](https://editor-next.swagger.io/?url=https://raw.githubusercontent.com/lancedb/lance-namespace/refs/heads/main/docs/src/rest.yaml). More details about the specification, please refer to docs [here](https://lance.org/format/namespace/impls/rest/)

Besides, Lance REST service can be run standalone without Gravitino server,



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

### Running Lance REST service with Gravitino

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

### Running Lance REST service standalone

To run Lance REST service standalone without Gravitino server, you can use the following command:

```shell
{GRAVITINO_HOME}/bin/gravitino-lance-rest-server.sh start
```

The following configurations are required to run Lance REST service standalone, you can set them in `gravitino-lance-rest-server.conf` file or pass them as command line arguments.
Typically, you only need to change the following configurations:

| Configuration item                             | Description                                                                        | Default value            | Required                                   | Since Version |
|------------------------------------------------|------------------------------------------------------------------------------------|--------------------------|--------------------------------------------|---------------|
| `gravitino.lance-rest.namespace-backend`       | backend to store namespace metadata, currently it only supports `gravitino`        | gravitino                | Yes if Lance REST service is going to run  | 1.1.0         |
| `gravitino.lance-rest.gravitino.uri`           | Gravitino server URI, it should be set when `namespace-backend` is `gravitino`     | http://localhost:8090    | Yes if Lance REST service is going to run  | 1.1.0         |
| `gravitino.lance-rest.gravitino.metalake-name` | Gravitino metalake name, it should be set when `namespace-backend` is `gravitino`  | (none)                   | Yes if Lance REST service is going to run  | 1.1.0         |
| `gravitino.lance-rest.port`                    | The port number that Lance REST service listens on.                                | 9101                     | Yes if Lance REST service is going to run  | 1.1.0         |
| `gravitino.lance-rest.host`                    | The host name that Lance REST service run in                                       | 0.0.0.0                  | Yes if Lance REST service is going to run  | 1.1.0         |

`namespace-backend`, `uri`, `port` and `host` have the same meaning as described in the previous section, and they have the default values. In most cases you only need to change `metalake-name` to your Gravitino metalake name. 
For other configurations listed in the file, just keep their default values.

## Using Lance REST service

Currently, as the Lance REST service only support Gravitino backend, so there are some limitations when using Lance REST service:
- You need to have a running Gravitino server with a metalake created. 
- As Gravitino has three hierarchies: catalog -> schema -> table, so when you create namespaces or a table via Lance REST service, you need to make sure the parent hierarchy exists. For example, when you create a namespace `lance_catalog/schema`, you need to make sure the catalog `lance_catalog` already exists in Gravitino metalake. If not, you need to create the namespace(catalog) `lance_catalog` first.
- Currently, we can only support two layers of namespaces and then tables, that is to say, you can create namespace like `lance_catalog/schema`, but you cannot create namespace like `lance_catalog/schema/sub_schema`. Tables can only be created under the namespace `lance_catalog/schema`.

## Example

When Gravitino server is started with Lance REST service starts successfully, and a `generic-lakehouse` catalog named `lance_catalog` is created in Gravitino metalake, you can use the following Python code to interact with Lance REST service:



<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
# Create a namespace
# mode can be create or exist_ok or overwrite
curl -X POST http://localhost:9101/lance/v1/namespace/lance_catalog/create -H 'Content-Type: application/json' -d '{
    "id": ["lance_catalog"],
    "mode": "create"
}'

# Create a schema namespace
# %24 is the URL encoded character for $
curl -X POST http://localhost:9101/lance/v1/namespace/lance_catalog%24schema/create -H 'Content-Type: application/json' -d '{
    "id": ["lance_catalog", "schema"],
    "mode": "create"
}'

# register a table
curl -X POST http://localhost:9101/lance/v1/table/lance_catalog2%24schema%24table01/register -H 'Content-Type: application/json' -d '{
    "id": ["lance_catalog","schema","table01"],
    "location": "/tmp/lance_catalog/schema/table01"
}'

```

</TabItem>
<TabItem value="java" label="Java">

```java
// implementation("com.lancedb:lance-namespace-core:0.0.19")

private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
LanceNamespace ns = LanceNamespace.connect("rest", Map.of("uri", "http://localhost:9101/lance"));
HashMap<String, String> props = Maps.newHashMap();
props.put(RestNamespaceConfig.URI, getLanceRestServiceUrl());
props.put(RestNamespaceConfig.DELIMITER, RestNamespaceConfig.DELIMITER_DEFAULT);
LanceNamespace ns = LanceNamespaces.connect("rest", props, null, allocator);

// Create a namespace
CreateNamespaceRequest createCatalogNsRequest = new CreateNamespaceRequest();
createCatalogNsRequest.addIdItem("lance_catalog");
createCatalogNsRequest.setMode(CreateNamespaceRequest.ModeEnum.CREATE);
ns.createNamespace(createCatalogNsRequest);

// Create a schema namespace
CreateNamespaceRequest createSchemaNsRequest = new CreateNamespaceRequest();
createSchemaNsRequest.addIdItem("lance_catalog");
createSchemaNsRequest.addIdItem("schema");
createSchemaNsRequest.setMode(CreateNamespaceRequest.ModeEnum.CREATE);
ns.createNamespace(createSchemaNsRequest);  

// register a table
RegisterTableRequest registerTableRequest = new RegisterTableRequest();
registerTableRequest.setLocation(location);
registerTableRequest.setId(Lists.newArrayList("lance_catalog", "schema", "table01"));
ns.registerTable(registerTableRequest);
```

</TabItem>
<TabItem value="python" label="Python">

```python
# you need to install lance-namespace package first by 'pip install lance-namespace==0.0.20'
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

</TabItem>
</Tabs>


## Using Lance REST service with Docker

You can also run Lance REST service with Docker. The following is an example command to run Lance REST service with Docker:

```shell
docker run -d --name lance-rest-service -p 9101:9101 \
  -e LANCE_REST_GRAVITINO_URI=http://gravitino-host:8090 \
  -e LANCE_REST_GRAVITINO_METALAKE_NAME=your_metalake_name \
  apache/gravitino-lance-rest:latest
```       

Then you can access Lance REST service at `http://localhost:9101`.

The following environment variables are used to configure Lance REST service in Docker:

| Environment variables                | Configuration items                                           | Required | Default                 | Since version |
|--------------------------------------|---------------------------------------------------------------|----------|-------------------------|---------------|
| `LANCE_REST_GRAVITINO_METALAKE_NAME` | `gravitino.lance-rest.gravitino.metalake-name`                | Y        | (none)                  | 1.1.0         |
| `LANCE_REST_NAMESPACE_BACKEND`       | `gravitino.lance-rest.gravitino.lance-rest.namespace-backend` | N        | `gravitino`             | 1.1.0         |
| `LANCE_REST_GRAVITINO_URI`           | `gravitino.lance-rest.gravitino-uri`                          | N        | `http://localhost:8090` | 1.1.0         |
| `LANCE_REST_HOST`                    | `gravitino.lance-rest.host`                                   | N        | `0.0.0.0`               | 1.1.0         |
| `LANCE_REST_PORT`                    | `gravitino.lance-rest.httpPort`                               | N        | `9101`                  | 1.1.0         |

- In most cases, you only need to set `LANCE_REST_GRAVITINO_METALAKE_NAME` to your Gravitino metalake name.
- If your Gravitino server is not running on `localhost`, you need to set `LANCE_REST_GRAVITINO_URI` to the correct Gravitino server URI.
- Other three environment variables are optional, you can keep their default values if there are no special requirements.


