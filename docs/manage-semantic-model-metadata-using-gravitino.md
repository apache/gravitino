---
title: "Manage Semantic Model Metadata"
slug: "/manage-semantic-model-metadata-using-gravitino"
keyword: "semantic model management, semantic metadata, Ossie, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

This page covers the Gravitino API for Semantic Models. For what a model contains, how its sources
are validated, and how it relates to tables and views, see [Semantic Models](./semantic-models.md).
For creating the catalog and schema a model lives in, see
[Manage Catalogs and Schemas](./manage-catalogs-and-schemas.md).

## Before You Start

The examples use the `2.0.0` server and clients. They assume:

* A server at `http://localhost:8090` and an existing metalake named `example`.
* A relational catalog named `sales`, with a schema named `mart`.
* An existing source table `sales.mart.orders` with `order_id`, `customer_id`, and `order_amount` columns.
* An existing source table `sales.mart.customers` with `customer_id` and `customer_code` columns.

See [Manage Relational Metadata](./manage-relational-metadata-using-gravitino.md) to create the
source tables. Add the credentials required by your deployment as described in
[Authentication](./security/how-to-authenticate.md). With authorization enabled, the caller also
needs the [model and source privileges](#ownership-and-privileges).

Choose one language tab and follow its examples. Each language manages the same model names, so
running create or import again in another language reports that the model already exists.
Run the [drop examples](#drop-a-semantic-model) when you have finished.

Java and Python model identifiers are relative to the loaded catalog (`schema.model`). Dataset
sources use `catalog.schema.object` within the same metalake. For SDK configuration, see
[Client Setup](./how-to-use-gravitino-client.md) and [Python Client](./how-to-use-python-client.md).

<details>
<summary>Java and Python client setup and imports</summary>

Place the operation snippets inside the client block below. The block closes the client when
finished; Java file operations also require the calling method to handle or declare `IOException`.

<Tabs groupId='language' queryString>
<TabItem value="java" label="Java">

```java
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dialects;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelCatalog;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;

try (GravitinoClient client = GravitinoClient.builder("http://localhost:8090")
    .withMetalake("example").build()) {
  SemanticModelCatalog models = client.loadCatalog("sales").asSemanticModelCatalog();
  NameIdentifier ident = NameIdentifier.of("mart", "sales_model");

  // Run the Java operation examples here.
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
from pathlib import Path

from gravitino import GravitinoClient, NameIdentifier
from gravitino.api.semantic.data_type import DataType
from gravitino.api.semantic.dataset import Dataset
from gravitino.api.semantic.dialect_expression import DialectExpression
from gravitino.api.semantic.dialects import Dialects
from gravitino.api.semantic.expression import Expression
from gravitino.api.semantic.field import Field
from gravitino.api.semantic.metric import Metric
from gravitino.api.semantic.relationship import Relationship
from gravitino.api.semantic.semantic_model_change import SemanticModelChange
from gravitino.api.semantic.semantic_model_definition import SemanticModelDefinition
from gravitino.namespace import Namespace

client = GravitinoClient(uri="http://localhost:8090", metalake_name="example")
try:
    models = client.load_catalog("sales").as_semantic_model_catalog()
    ident = NameIdentifier.of("mart", "sales_model")

    # Run the Python operation examples here.
finally:
    client.close()
```

</TabItem>
</Tabs>

</details>

## Semantic Model Operations

### Create a Semantic Model

A model needs a name and a definition with at least one dataset. This example defines a revenue
metric over orders and links orders to customers by `customer_id`. The native REST API nests the
definition under `definition`. Its field names include `primaryKey` and `datatype`.

The relationship's `from` and `to` name datasets in the model. Its column lists and the datasets'
keys use source table column names, even when those columns are not listed in `fields`. Each array
inside `uniqueKeys` defines one unique key.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "name": "sales_model",
  "comment": "Governed sales definitions",
  "properties": {"domain": "sales"},
  "definition": {
    "datasets": [
      {
        "name": "orders",
        "source": {"namespace": ["sales", "mart"], "name": "orders"},
        "primaryKey": ["order_id"],
        "fields": [
          {
            "name": "order_amount",
            "expression": {
              "dialects": [{"dialect": "ANSI_SQL", "expression": "order_amount"}]
            },
            "datatype": "Decimal"
          }
        ]
      },
      {
        "name": "customers",
        "source": {"namespace": ["sales", "mart"], "name": "customers"},
        "primaryKey": ["customer_id"],
        "uniqueKeys": [["customer_code"]],
        "fields": []
      }
    ],
    "relationships": [
      {
        "name": "orders_to_customers",
        "from": "orders",
        "to": "customers",
        "fromColumns": ["customer_id"],
        "toColumns": ["customer_id"]
      }
    ],
    "metrics": [
      {
        "name": "total_revenue",
        "expression": {
          "dialects": [{"dialect": "ANSI_SQL", "expression": "SUM(orders.order_amount)"}]
        },
        "datatype": "Decimal"
      }
    ]
  }
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/mart/semantic-models
```

</TabItem>
<TabItem value="java" label="Java">

```java
Expression amountExpression = Expression.builder()
    .withDialects(new DialectExpression[] {
        DialectExpression.builder().withDialect(Dialects.ANSI_SQL)
            .withExpression("order_amount").build()
    }).build();
Field amount = Field.builder().withName("order_amount")
    .withExpression(amountExpression).withDatatype(DataType.DECIMAL).build();
Dataset orders = Dataset.builder().withName("orders")
    .withSource(NameIdentifier.of("sales", "mart", "orders"))
    .withPrimaryKey(new String[] {"order_id"})
    .withFields(new Field[] {amount}).build();
Dataset customers = Dataset.builder().withName("customers")
    .withSource(NameIdentifier.of("sales", "mart", "customers"))
    .withPrimaryKey(new String[] {"customer_id"})
    .withUniqueKeys(new String[][] {{"customer_code"}})
    .withFields(new Field[0]).build();
Relationship ordersToCustomers = Relationship.builder().withName("orders_to_customers")
    .withFrom("orders").withTo("customers")
    .withFromColumns(new String[] {"customer_id"})
    .withToColumns(new String[] {"customer_id"}).build();

Expression revenueExpression = Expression.builder()
    .withDialects(new DialectExpression[] {
        DialectExpression.builder().withDialect(Dialects.ANSI_SQL)
            .withExpression("SUM(orders.order_amount)").build()
    }).build();
Metric revenue = Metric.builder().withName("total_revenue")
    .withExpression(revenueExpression).withDatatype(DataType.DECIMAL).build();
SemanticModelDefinition definition = SemanticModelDefinition.builder()
    .withDatasets(new Dataset[] {orders, customers})
    .withRelationships(new Relationship[] {ordersToCustomers})
    .withMetrics(new Metric[] {revenue}).build();

SemanticModel created = models.createSemanticModel(
    ident, "Governed sales definitions", definition, Map.of("domain", "sales"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
amount = Field(
    "order_amount",
    Expression([DialectExpression(Dialects.ANSI_SQL, "order_amount")]),
    datatype=DataType.DECIMAL)
orders = Dataset(
    "orders",
    NameIdentifier.of("sales", "mart", "orders"),
    primary_key=["order_id"],
    fields=[amount])
customers = Dataset(
    "customers",
    NameIdentifier.of("sales", "mart", "customers"),
    primary_key=["customer_id"],
    unique_keys=[["customer_code"]],
    fields=[])
orders_to_customers = Relationship(
    "orders_to_customers",
    from_dataset="orders",
    to_dataset="customers",
    from_columns=["customer_id"],
    to_columns=["customer_id"])
revenue = Metric(
    "total_revenue",
    Expression([DialectExpression(Dialects.ANSI_SQL, "SUM(orders.order_amount)")]),
    datatype=DataType.DECIMAL)
definition = SemanticModelDefinition(
    datasets=[orders, customers],
    relationships=[orders_to_customers],
    metrics=[revenue])

created = models.create_semantic_model(
    ident, "Governed sales definitions", definition, {"domain": "sales"})
```

</TabItem>
</Tabs>

The REST response contains `code: 0` and a `semanticModel` object with `name`, `comment`,
`properties`, `definition`, and `audit`. The SDKs return a `SemanticModel` directly. Creating a name
that already exists reports a conflict without overwriting the model.

### List Semantic Models

Listing returns identifiers rather than complete definitions. With authorization enabled, it
includes only models visible to the caller.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/mart/semantic-models
```

</TabItem>
<TabItem value="java" label="Java">

```java
NameIdentifier[] identifiers = models.listSemanticModels(Namespace.of("mart"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
identifiers = models.list_semantic_models(Namespace.of("mart"))
```

</TabItem>
</Tabs>

REST identifiers include the namespace `["example", "sales", "mart"]`. SDK identifiers are relative
to the loaded catalog, such as `mart.sales_model`.

### Load a Semantic Model

Load returns the stored definition, properties, comment, and audit information.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X GET -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/mart/semantic-models/sales_model
```

</TabItem>
<TabItem value="java" label="Java">

```java
SemanticModel loaded = models.loadSemanticModel(ident);
```

</TabItem>
<TabItem value="python" label="Python">

```python
loaded = models.load_semantic_model(ident)
```

</TabItem>
</Tabs>

### Alter a Semantic Model

Changes are applied in order in one request. If any change or the resulting model is invalid, none
of the changes is persisted.

| Change | REST update |
|--------|-------------|
| Rename within the schema | `{"@type":"rename","newName":"renamed_model"}` |
| Update or clear the comment | `{"@type":"updateComment","newComment":"Reviewed sales definitions"}`; use `null` to clear it. |
| Set a property | `{"@type":"setProperty","property":"domain","value":"finance"}` |
| Remove a property | `{"@type":"removeProperty","property":"domain"}` |
| Replace the definition | Set `@type` to `replaceDefinition` and supply the complete native definition in `definition`. |

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X PUT -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{
  "updates": [
    {"@type": "updateComment", "newComment": "Reviewed sales definitions"},
    {"@type": "setProperty", "property": "domain", "value": "finance"}
  ]
}' http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/mart/semantic-models/sales_model
```

</TabItem>
<TabItem value="java" label="Java">

```java
SemanticModel altered = models.alterSemanticModel(
    ident,
    SemanticModelChange.updateComment("Reviewed sales definitions"),
    SemanticModelChange.setProperty("domain", "finance"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
altered = models.alter_semantic_model(
    ident,
    SemanticModelChange.update_comment("Reviewed sales definitions"),
    SemanticModelChange.set_property("domain", "finance"))
```

</TabItem>
</Tabs>

Definition replacement replaces all datasets, relationships, and metrics. Include everything you
want to retain. In Java, use `SemanticModelChange.replaceDefinition(definition)`; in Python, use
`SemanticModelChange.replace_definition(definition)`. Each replacement revalidates the complete
definition and its source access. Comment and property changes do not revalidate sources.

## Apache Ossie Import and Export

An Ossie document uses snake_case names, string source identifiers, and root-level datasets,
relationships, and metrics. See [Exchanging Definitions](./semantic-models.md#exchanging-definitions)
for the mapping from the native REST format.

### Import a Document

Save the following as `sales.ossie.yaml`. It uses a different model name so it can be imported
alongside the model created above:

```yaml
version: 0.2.0.dev0
name: imported_sales_model
description: Imported sales definitions
datasets:
  - name: orders
    source: sales.mart.orders
    primary_key: [order_id]
    fields:
      - name: order_amount
        expression:
          dialects:
            - dialect: ANSI_SQL
              expression: order_amount
        datatype: Decimal
  - name: customers
    source: sales.mart.customers
    primary_key: [customer_id]
    unique_keys: [[customer_code]]
    fields: []
relationships:
  - name: orders_to_customers
    from: orders
    to: customers
    from_columns: [customer_id]
    to_columns: [customer_id]
metrics:
  - name: total_revenue
    expression:
      dialects:
        - dialect: ANSI_SQL
          expression: SUM(orders.order_amount)
    datatype: Decimal
custom_extensions:
  - vendor_name: GRAVITINO_PROPERTIES
    data: '{"domain":"sales"}'
```

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/yaml" --data-binary @sales.ossie.yaml \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/mart/semantic-models/ossie
```

</TabItem>
<TabItem value="java" label="Java">

```java
SemanticModel importedModel = models.importOssieDocument(
    Namespace.of("mart"), OssieDocument.yaml(Files.readString(Path.of("sales.ossie.yaml"))));
```

</TabItem>
<TabItem value="python" label="Python">

```python
imported_model = models.import_ossie_document(
    Namespace.of("mart"), Path("sales.ossie.yaml").read_text(encoding="utf-8"))
```

</TabItem>
</Tabs>

Import creates a new model using the name in the document and applies the same validation as
native creation. An existing name reports a conflict. REST returns the native `semanticModel`
response; the SDKs return a `SemanticModel`.

Send the raw document text. For JSON, use `Content-Type: application/json` in REST,
`OssieDocument.json(...)` in Java, or `document_format="json"` in Python. YAML requests use
`application/yaml`; `application/x-yaml` and `text/yaml` are also accepted. An omitted content type
defaults to YAML, while an unsupported content type returns `415`. Use an explicit content type
with curl, since its data options otherwise send a form content type.

### Export a Document

Export the native model as an Ossie JSON document:

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl 'http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/mart/semantic-models/sales_model/ossie?format=json' \
  -o exported-sales.ossie.json
```

</TabItem>
<TabItem value="java" label="Java">

```java
OssieDocument exported = models.exportOssieDocument(ident, OssieFormat.JSON);
Files.writeString(Path.of("exported-sales.ossie.json"), exported.content());
```

</TabItem>
<TabItem value="python" label="Python">

```python
exported = models.export_ossie_document(ident, document_format="json")
Path("exported-sales.ossie.json").write_text(exported, encoding="utf-8")
```

</TabItem>
</Tabs>

For YAML, use `format=yaml` in REST, `OssieFormat.YAML` in Java, or `document_format="yaml"` in
Python. REST and Python default to YAML when the format is omitted. REST returns the document
text without the native response wrapper, and Python returns that text unchanged.

The `ossie-version` model property supplies the document's `version`. Native creation records the
server's default when the property is omitted; import records the document version. Other model
properties are exchanged through the root `GRAVITINO_PROPERTIES` custom extension, whose `data`
is a JSON object encoded as a string. Its values must be strings. Use `properties` when creating a
model through the native API; the reserved extension is for Ossie exchange.

## Ownership and Privileges

When authorization is enabled, a model's creator becomes its owner. For callers using explicit
privilege grants, access also requires `USE_CATALOG` and `USE_SCHEMA` on the parent objects:

| Operation | Required model privilege or ownership |
|-----------|---------------------------------------|
| Create or import | `CREATE_SEMANTIC_MODEL` on the containing schema or an ancestor. |
| Discover, load, or export | `USE_SEMANTIC_MODEL`, `MODIFY_SEMANTIC_MODEL`, or ownership. |
| Alter or rename | `MODIFY_SEMANTIC_MODEL` or ownership. |
| Drop | Ownership; `MODIFY_SEMANTIC_MODEL` alone is insufficient. |

Parent owners can also operate on contained models under the
[access-control rules](./security/access-control.md#required-privileges). A schema owner still
needs access to the catalog. Create, import, and definition replacement additionally check the
caller's access to every source table or view. Permission to load a Semantic Model does not itself
grant permission to read its source data.

Ownership and role grants use metadata object type `SEMANTIC_MODEL` and a name such as
`sales.mart.sales_model` within the selected metalake. See
[Ownership](./security/access-control.md#ownership) and the
[access-control walkthrough](./security/access-control.md#walkthrough) to manage owners and roles.
Setting a string property named `owner` does not transfer ownership.

### Tags and Policies

Create a tag in the metalake before attaching it to a Semantic Model. For an existing tag named
`certified`, use the `semantic_model` object type in the REST path:

```shell
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" -d '{"tagsToAdd": ["certified"]}' \
  http://localhost:8090/api/metalakes/example/objects/semantic_model/sales.mart.sales_model/tags
```

Tag operations have their own authorization requirements. See
[Manage Tags](./manage-tags-in-gravitino.md#object-operations) for creating, attaching, listing, and
removing tags. Policies are selected through tags; see
[Policy-to-Tag Associations](./manage-policies-in-gravitino.md#policy-to-tag-associations).

## Drop a Semantic Model

Drop the models after trying the examples. Dropping removes the semantic metadata and its
governance associations without deleting source tables, views, or data.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/mart/semantic-models/sales_model
curl -X DELETE -H "Accept: application/vnd.gravitino.v1+json" \
  http://localhost:8090/api/metalakes/example/catalogs/sales/schemas/mart/semantic-models/imported_sales_model
```

</TabItem>
<TabItem value="java" label="Java">

```java
boolean dropped = models.dropSemanticModel(ident);
boolean importedDropped = models.dropSemanticModel(
    NameIdentifier.of("mart", "imported_sales_model"));
```

</TabItem>
<TabItem value="python" label="Python">

```python
dropped = models.drop_semantic_model(ident)
imported_dropped = models.drop_semantic_model(
    NameIdentifier.of("mart", "imported_sales_model"))
```

</TabItem>
</Tabs>

REST returns `{"code":0,"dropped":true}` when the model is dropped, while the SDKs return a boolean.
An authorized request for an absent model returns `false` (`dropped: false` in REST).

## Other Interfaces

The [Gravitino MCP server](./gravitino-mcp-server.md#semantic-models) provides
`list_of_semantic_models`, `load_semantic_model`, `create_semantic_model`, `alter_semantic_model`,
and `drop_semantic_model`. Enable the `semantic_model` tool tag when filtering tools.
Create and alter use the native REST definition format. Use REST or the SDKs for Ossie import and
export. The current Web UI does not provide Semantic Model workflows.
