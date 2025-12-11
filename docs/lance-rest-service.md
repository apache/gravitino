---
title: "Lance REST service"
slug: /lance-rest-service
keywords:
  - Lance REST
  - Lance datasets
  - REST API
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Overview

The Lance REST service provides a RESTful interface for managing Lance datasets through HTTP endpoints. Introduced in Gravitino version 1.1.0, this service enables seamless interaction with Lance datasets for data operations and metadata management.

The service implements the [Lance REST API specification](https://docs.lancedb.com/api-reference/introduction). For detailed specification documentation, see the [official Lance REST documentation](https://lance.org/format/namespace/rest/catalog-spec/).

### What is Lance?

[Lance](https://lance.org/format/) is a modern columnar data format designed for AI/ML workloads. It provides:

- **High-performance vector search**: Native support for similarity search on high-dimensional embeddings
- **Columnar storage**: Optimized for analytical queries and machine learning pipelines
- **Fast random access**: Efficient row-level operations unlike traditional columnar formats
- **Version control**: Built-in dataset versioning and time-travel capabilities
- **Incremental updates**: Append and update data without full rewrites

### Architecture

The Lance REST service acts as a bridge between Lance datasets and applications:

```
┌─────────────────┐
│   Applications   │
│  (Python/Java)   │
└────────┬────────┘
         │ HTTP/REST
         ▼
┌─────────────────┐
│   Lance REST     │
│    Service       │ 
└────────┬────────┘
         │ 
         ▼ Gravitino Client API
┌─────────────────┐
│ Gravitino Server │ 
│(Metadata Backend)│
└────────┬────────┘
         │ File System Operations
         ▼
┌─────────────────┐
│  Lance Datasets  │ 
│ (S3/GCS/Local)   │
└─────────────────┘
```

**Key Features:**
- Full compliance with Lance REST API specification
- Can run standalone or integrated with Gravitino server
- Support for namespace and table management
- Index creation and management capabilities (Index operations are not supported in version 1.1.0)
- Metadata stored in Gravitino for unified governance

## Supported Operations

The Lance REST service provides comprehensive support for namespace management, table management, and index operations. The table below lists all supported operations:

| Operation         | Description                                                       | HTTP Method | Endpoint Pattern                      | Since Version |
|-------------------|-------------------------------------------------------------------|-------------|---------------------------------------|---------------|
| CreateNamespace   | Create a new Lance namespace                                      | POST        | `/lance/v1/namespace/{id}/create`     | 1.1.0         |
| ListNamespaces    | List all namespaces under a parent namespace                      | GET         | `/lance/v1/namespace/{parent}/list`   | 1.1.0         |
| DescribeNamespace | Retrieve detailed information about a specific namespace          | POST        | `/lance/v1/namespace/{id}/describe`   | 1.1.0         |
| DropNamespace     | Delete a namespace                                                | POST        | `/lance/v1/namespace/{id}/drop`       | 1.1.0         |
| NamespaceExists   | Check whether a namespace exists                                  | POST        | `/lance/v1/namespace/{id}/exists`     | 1.1.0         |
| ListTables        | List all tables in a namespace                                    | GET         | `/lance/v1/namespace/{id}/table/list` | 1.1.0         |
| CreateTable       | Create a new table in a namespace                                 | POST        | `/lance/v1/table/{id}/create`         | 1.1.0         |
| DropTable         | Delete a table including both metadata and data                   | POST        | `/lance/v1/table/{id}/drop`           | 1.1.0         |
| TableExists       | Check whether a table exists                                      | POST        | `/lance/v1/table/{id}/exists`         | 1.1.0         |
| RegisterTable     | Register an existing Lance table to a namespace                   | POST        | `/lance/v1/table/{id}/register`       | 1.1.0         |
| DeregisterTable   | Unregister a table from a namespace (metadata only, data remains) | POST        | `/lance/v1/table/{id}/deregister`     | 1.1.0         |

More details, please refer to the [Lance REST API specification](https://lance.org/format/namespace/rest/catalog-spec/)

### Operation Details

Some operations have specific behaviors and modes. Below are important details to consider:

#### Namespace Operations

**CreateNamespace** supports three modes:
- `create`: Fails if namespace already exists
- `exist_ok`: Succeeds even if namespace exists  
- `overwrite`: Replaces existing namespace

**DropNamespace** behavior:
- Recursively deletes all child namespaces and tables
- Deletes both metadata and Lance data files
- Operation is irreversible

#### Table Operations

**RegisterTable vs CreateTable**:
- **RegisterTable**: Links existing Lance datasets into Gravitino catalog without data movement
- **CreateTable**: Creates new Lance table with schema and write metadata files
:::note
The `version` field of `CreateTable` response is always null, which stands for the latest version. 
:::

**DropTable vs DeregisterTable**:
- **DropTable**: Permanently deletes metadata and data files from storage
- **DeregisterTable**: Removes metadata from Gravitino but preserves Lance data files


## Deployment

### Running with Gravitino Server

To enable the Lance REST service within Gravitino server, configure the following properties in your Gravitino configuration file `${GRAVITINO_HOME}/conf/gravitino.conf`:

| Configuration Property                    | Description                                                                  | Default Value           | Required | Since Version |
|-------------------------------------------|------------------------------------------------------------------------------|-------------------------|----------|---------------|
| `gravitino.auxService.names`              | Auxiliary services to run. Include `lance-rest` to enable Lance REST service | iceberg-rest,lance-rest | Yes      | 0.2.0         |
| `gravitino.lance-rest.classpath`          | Classpath for Lance REST service, relative to Gravitino home directory       | lance-rest-server/libs  | Yes      | 1.1.0         |
| `gravitino.lance-rest.httpPort`           | Port number for Lance REST service                                           | 9101                    | No       | 1.1.0         |
| `gravitino.lance-rest.host`               | Hostname for Lance REST service                                              | 0.0.0.0                 | No       | 1.1.0         |
| `gravitino.lance-rest.namespace-backend`  | Namespace metadata backend (currently only `gravitino` is supported)         | gravitino               | Yes      | 1.1.0         |
| `gravitino.lance-rest.gravitino-uri`      | Gravitino server URI (required when namespace-backend is `gravitino`)        | http://localhost:8090   | Yes      | 1.1.0         |
| `gravitino.lance-rest.gravitino-metalake` | Gravitino metalake name (required when namespace-backend is `gravitino`)     | (none)                  | Yes      | 1.1.0         |

**Example Configuration:**

```properties
gravitino.auxService.names = lance-rest
gravitino.lance-rest.httpPort = 9101
gravitino.lance-rest.host = 0.0.0.0
gravitino.lance-rest.namespace-backend = gravitino
gravitino.lance-rest.gravitino-uri = http://localhost:8090
gravitino.lance-rest.gravitino-metalake = my_metalake
```

### Running Standalone

To run Lance REST service independently without Gravitino server (You need to start Gravitino server first):

```shell
{GRAVITINO_HOME}/bin/gravitino-lance-rest-server.sh start
```

Configure the service by editing `{GRAVITINO_HOME}/conf/gravitino-lance-rest-server.conf` or passing command-line arguments:

| Configuration Property                    | Description                 | Default Value         | Required | Since Version |
|-------------------------------------------|-----------------------------|-----------------------|----------|---------------|
| `gravitino.lance-rest.namespace-backend`  | Namespace metadata backend  | gravitino             | Yes      | 1.1.0         |
| `gravitino.lance-rest.gravitino-uri`      | Gravitino server URI        | http://localhost:8090 | Yes      | 1.1.0         |
| `gravitino.lance-rest.gravitino-metalake` | Gravitino metalake name     | (none)                | Yes      | 1.1.0         |
| `gravitino.lance-rest.httpPort`           | Service port number         | 9101                  | No       | 1.1.0         |
| `gravitino.lance-rest.host`               | Service hostname            | 0.0.0.0               | No       | 1.1.0         |

:::tip
In most cases, you only need to configure `gravitino.lance-rest.gravitino-metalake` and other properties can use their default values.
:::


### Running with Docker

Launch Lance REST service using Docker(You need to start Gravitino server first):

```shell
docker run -d --name lance-rest-service -p 9101:9101 \
  -e LANCE_REST_GRAVITINO_URI=http://gravitino-host:8090 \
  -e LANCE_REST_GRAVITINO_METALAKE_NAME=your_metalake_name \
  -e LANCE_REST_GRAVITINO_URI=http://gravitino-host:port \
  apache/gravitino-lance-rest:latest
```

Access the service at `http://localhost:9101`.

**Environment Variables:**

| Environment Variable                 | Configuration Property                    | Required | Default Value           | Since Version |
|--------------------------------------|-------------------------------------------|----------|-------------------------|---------------|
| `LANCE_REST_NAMESPACE_BACKEND`       | `gravitino.lance-rest.namespace-backend`  | Yes      | `gravitino`             | 1.1.0         |
| `LANCE_REST_GRAVITINO_METALAKE_NAME` | `gravitino.lance-rest.gravitino-metalake` | Yes      | (none)                  | 1.1.0         |
| `LANCE_REST_GRAVITINO_URI`           | `gravitino.lance-rest.gravitino-uri`      | Yes      | `http://localhost:8090` | 1.1.0         |
| `LANCE_REST_HOST`                    | `gravitino.lance-rest.host`               | No       | `0.0.0.0`               | 1.1.0         |
| `LANCE_REST_PORT`                    | `gravitino.lance-rest.httpPort`           | No       | `9101`                  | 1.1.0         |

:::tip Configuration Tips
- **Required:** Set `LANCE_REST_GRAVITINO_METALAKE_NAME` to your Gravitino metalake name
- **Conditional:** Update `LANCE_REST_GRAVITINO_URI` if Gravitino server is not on `localhost` in the docker instance.
- **Optional:** Other variables can use default values unless you have specific requirements
:::

## Usage Guidelines

When using Lance REST service with Gravitino backend, keep the following considerations in mind:

### Prerequisites
- A running Gravitino server with a created metalake

### Namespace Hierarchy
Gravitino follows a three-level hierarchy: **catalog → schema → table**. When creating namespaces or tables:

1. **Parent must exist:** Before creating `lance_catalog/schema`, ensure `lance_catalog` catalog exists in Gravitino metalake.
2. **Two-level limit:** You can create namespace `lance_catalog/schema`, but **not** `lance_catalog/schema/sub_schema`.
3. **Table placement:** Tables can only be created under `lance_catalog/schema`, not at catalog level.

**Example Hierarchy:**
```
metalake
└── lance_catalog (catalog - create via REST)
    └── schema (namespace - create via REST)
        └── table01 (table - create via REST)
```

### Delimiter Convention

The Lance REST API uses `$` as the default delimiter to separate namespace levels in URIs. When making HTTP requests:

- **URL Encoding Required**: `$` must be URL-encoded as `%24`
- **Example**: `lance_catalog$schema$table01` becomes `lance_catalog%24schema%24table01` in URLs

**Common Delimiters:**
```
Namespace path:     lance_catalog.schema.table01
URI representation: lance_catalog$schema$table01  
URL encoded:        lance_catalog%24schema%24table01
```

:::caution Important Limitations
- Currently supports only **two levels of namespaces** before tables
- Tables **cannot** be nested deeper than schema level  
- Parent catalog must be created in Gravitino before using Lance REST API
- Metadata operations require Gravitino server to be available
- Namespace deletion is recursive and irreversible
:::

## Examples

The following examples demonstrate how to interact with Lance REST service using different programming languages and tools.

**Prerequisites:**
- Gravitino server is running with Lance REST service enabled.
- A metalake has been created in Gravitino.

<Tabs groupId="language" queryString>
<TabItem value="shell" label="Shell">

```shell
# Create a catalog-level namespace
# mode: "create" | "exist_ok" | "overwrite" for create namespace/table; mode: "create" | "overwrite" for register table
curl -X POST http://localhost:9101/lance/v1/namespace/lance_catalog/create \
  -H 'Content-Type: application/json' \
  -d '{
    "id": ["lance_catalog"],
    "mode": "create"
  }'

# Create a schema namespace
# Note: %24 is URL-encoded '$' character used as delimiter
curl -X POST http://localhost:9101/lance/v1/namespace/lance_catalog%24schema/create \
  -H 'Content-Type: application/json' \
  -d '{
    "id": ["lance_catalog", "schema"],
    "mode": "create"
  }'

# Register an existing table
curl -X POST http://localhost:9101/lance/v1/table/lance_catalog%24schema%24table01/register \
  -H 'Content-Type: application/json' \
  -d '{
    "id": ["lance_catalog", "schema", "table01"],
    "location": "/tmp/lance_catalog/schema/table01",
    "mode": "CREATE"
  }'

# Create a new empty table
curl -X POST http://localhost:9101/lance/v1/table/lance_catalog%24schema%24table02/create-empty \
  -H 'Content-Type: application/json' \
  -d '{
    "id": ["lance_catalog", "schema", "table02"],
    "location": "/tmp/lance_catalog/schema/table02",
    "properties": { "description": "This is table02"  }
  }'  
  
# Create a table with schema, the schema is inferred from the Arrow IPC file
curl -X POST \
     "http://localhost:9101/lance/v1/table/lance_catalog%24schema%24table03/create" \
     -H 'Content-Type: application/vnd.apache.arrow.stream' \
     -H "x-lance-table-location: /tmp/lance_catalog/schema/table03" \
     -H "x-lance-table-properties: {}" \
     --data-binary "@${ARROW_FILE}"    
```

</TabItem>
<TabItem value="java" label="Java">

```java
// Add dependency: implementation("com.lancedb:lance-namespace-core:0.0.20")

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import java.util.HashMap;
import java.util.Map;

// Initialize allocator and namespace connection
private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

Map<String, String> props = new HashMap<>();
props.put(RestNamespaceConfig.URI, "http://localhost:9101/lance");
props.put(RestNamespaceConfig.DELIMITER, RestNamespaceConfig.DELIMITER_DEFAULT);

LanceNamespace ns = LanceNamespaces.connect("rest", props, null, allocator);

// Create catalog namespace
CreateNamespaceRequest createCatalogNsRequest = new CreateNamespaceRequest();
createCatalogNsRequest.addIdItem("lance_catalog");
createCatalogNsRequest.setMode(CreateNamespaceRequest.ModeEnum.CREATE);
ns.createNamespace(createCatalogNsRequest);

// Create schema namespace
CreateNamespaceRequest createSchemaNsRequest = new CreateNamespaceRequest();
createSchemaNsRequest.addIdItem("lance_catalog");
createSchemaNsRequest.addIdItem("schema");
createSchemaNsRequest.setMode(CreateNamespaceRequest.ModeEnum.CREATE);
ns.createNamespace(createSchemaNsRequest);

// Register a table
RegisterTableRequest registerTableRequest = new RegisterTableRequest();
registerTableRequest.setLocation("/tmp/lance_catalog/schema/table01");
registerTableRequest.setId(Lists.newArrayList("lance_catalog", "schema", "table01"));
registerTableRequest.setMode(RegisterTableRequest.ModeEnum.CREATE);
ns.registerTable(registerTableRequest);

// Create an empty table
CreateEmptyTableRequest createEmptyTableRequest = new CreateEmptyTableRequest();
createEmptyTableRequest.setLocation("/tmp/lance_catalog/schema/table02");
createEmptyTableRequest.setId(Lists.newArrayList("lance_catalog", "schema", "table02"));
ns.createEmptyTable(createEmptyTableRequest);

// Create a table with schema inferred from Arrow IPC file
CreateTableRequest createTableRequest = new CreateTableRequest();
createTableRequest.setIds(Lists.newArrayList("lance_catalog", "schema", "table03"));
createTableRequest.setLocation("/tmp/lance_catalog/schema/table03");
org.apache.arrow.vector.types.pojo.Schema schema =
        new org.apache.arrow.vector.types.pojo.Schema(
                Arrays.asList(
                        Field.nullable("id", new ArrowType.Int(32, true)),
                        Field.nullable("value", new ArrowType.Utf8())));
byte[] body = ArrowUtils.generateIpcStream(schema);
ns.createTable(createTableRequest, body);

```

</TabItem>
<TabItem value="python" label="Python">

```python
# Install: pip install lance-namespace==0.0.20

import lance_namespace as ln

# Connect to Lance REST service
ns = ln.connect("rest", {"uri": "http://your_lance_rest:9101/lance"})

# Create catalog namespace
create_catalog_ns_request = ln.CreateNamespaceRequest(id=["lance_catalog"])
catalog = ns.create_namespace(create_catalog_ns_request)

# Create schema namespace
create_schema_ns_request = ln.CreateNamespaceRequest(id=["lance_catalog", "schema"])
schema = ns.create_namespace(create_schema_ns_request)

# Register a table
register_table_request = ln.RegisterTableRequest(
    id=['lance_catalog', 'schema', 'table01'],
    location='/tmp/lance_catalog/schema/table01'
)
ns.register_table(register_table_request)

# Create an empty table
create_empty_table_request = ln.CreateEmptyTableRequest(
    id=['lance_catalog', 'schema', 'table02'],
    location='/tmp/lance_catalog/schema/table02'
)

# Create a table with schema inferred from Arrow IPC file
create_table_request = ln.CreateTableRequest(
    id=['lance_catalog', 'schema', 'table03'],
    location='/tmp/lance_catalog/schema/table03'
)
with open('schema.ipc', 'rb') as f:
    body = f.read()

ns.create_table(create_table_request, body)
```

</TabItem>
</Tabs>
