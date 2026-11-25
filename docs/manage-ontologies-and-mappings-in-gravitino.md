---
title: "Manage Ontologies and Mappings"
slug: "/manage-ontologies-and-mappings-in-gravitino"
keyword: "ontology, mapping, semantic model, Ossie, REST, Java, Python, Gravitino"
license: "This software is licensed under the Apache License version 2."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Introduction

Map the `CustomerId` concept in a shared sales vocabulary to the `customers.customer_id` field
in a semantic model. The examples import two Apache Ossie documents, find the model that uses
the vocabulary, and export both documents as JSON. For scope and access rules, see
[Ontologies and Mappings](./ontologies-and-mappings.md).

## Prerequisites

Start with the [sales model example](./manage-semantic-model-metadata-using-gravitino.md#create-a-semantic-model).
It uses the `example` metalake on `http://localhost:8090` and a model named `sales_model` in
the `mart` schema of the `sales` catalog. The model declares `customer_id` in its `customers` dataset.

Use server and SDK builds that support ontologies and mappings. For client configuration, see
[Java client setup](./how-to-use-gravitino-client.md) or [Python client setup](./how-to-use-python-client.md).
Supply the [credentials](./security/how-to-authenticate.md) for your deployment.
Choose one language tab and follow it through cleanup. Each tab creates the same `Sales` ontology
and `customer_mapping` mapping, so choose an environment where those names are available.

## Ossie Documents

Save the ontology as `sales.ontology.yaml`. It defines one value type for an integer customer identifier.

```yaml
version: 0.3.0.dev0
name: Sales
ontology:
  - concept: CustomerId
    type: ValueType
    extends: [Integer]
```

Save the mapping as `customer.mapping.yaml` in the same directory.

```yaml
version: 0.3.0.dev0
name: customer_mapping
ontology_ref:
  name: Sales
semantic_model_ref:
  name: sales_model
concept_mappings:
  - concept: CustomerId
    object_mappings:
      - expression: customers.customer_id
```

Gravitino looks up `ontology_ref.name` in the model's metalake. The REST URL or the model loaded
by the SDK selects the parent model. The value of `semantic_model_ref.name` must match that model's
short name, `sales_model`. Passing `sales.mart.sales_model` in that field returns HTTP 400.
If another schema has a model with the same name, the catalog and schema in the request determine
which model receives the mapping.

## Client Setup

Run the examples from the directory containing the two YAML files. Keep the same shell for the REST
commands. For Java and Python, insert the later snippets at the indicated point in the client block.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
lake=http://localhost:8090/api/metalakes/example
model="$lake/catalogs/sales/schemas/mart/semantic-models/sales_model"
mappings="$model/ontology-mappings"
```

</TabItem>
<TabItem value="java" label="Java">

```java
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.ontology.SupportsOntologyMappings;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;

public class SalesMappingExample {
  public static void main(String[] args) throws IOException {
    try (GravitinoClient client = GravitinoClient.builder("http://localhost:8090")
        .withMetalake("example").build()) {
      SupportsOntologyMappings mappings = client.loadCatalog("sales").asSemanticModelCatalog()
          .loadSemanticModel(NameIdentifier.of("mart", "sales_model")).supportsOntologyMappings();
      // Insert the Java snippets here in order.
    }
  }
}
```

</TabItem>
<TabItem value="python" label="Python">

```python
from pathlib import Path

from gravitino import GravitinoClient, NameIdentifier

client = GravitinoClient(uri="http://localhost:8090", metalake_name="example")
try:
    models = client.load_catalog("sales").as_semantic_model_catalog()
    model = models.load_semantic_model(NameIdentifier.of("mart", "sales_model"))
    mappings = model.supports_ontology_mappings()
    # Insert the Python snippets here in order.
finally:
    client.close()
```

</TabItem>
</Tabs>

## Import the Ontology and Mapping

Import the ontology first so Gravitino can resolve `Sales` when it creates `customer_mapping`.
Both imports create new objects. Importing another mapping for the same model and ontology returns HTTP 409.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -sS -H 'Content-Type: application/yaml' \
  --data-binary @sales.ontology.yaml "$lake/ontologies/ossie"
curl --fail-with-body -sS -H 'Content-Type: application/yaml' \
  --data-binary @customer.mapping.yaml "$mappings/ossie"
```

</TabItem>
<TabItem value="java" label="Java">

```java
client.importOntology(OssieDocument.yaml(Files.readString(Path.of("sales.ontology.yaml"))));
mappings.importOntologyMapping(OssieDocument.yaml(Files.readString(Path.of("customer.mapping.yaml"))));
```

</TabItem>
<TabItem value="python" label="Python">

```python
client.import_ontology(Path("sales.ontology.yaml").read_text(encoding="utf-8"))
mappings.import_ontology_mapping(Path("customer.mapping.yaml").read_text(encoding="utf-8"))
```

</TabItem>
</Tabs>

For a JSON input file, use `Content-Type: application/json` in REST, `OssieDocument.json(...)` in Java,
or `document_format="json"` in Python. Ossie documents use `concept_mappings` and `object_mappings`.
Native create and update requests use `conceptMappings` and `objectMappings` under `definition`.
See the [Ontology and Mapping REST API](./open-api/ontologies.yaml) for the request schemas.

## Find Mappings for an Ontology

Look up `Sales` to find the semantic models that map to it.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -sS "$lake/ontologies/Sales/mappings"
```

The response includes the metalake in each identifier's namespace.

```json
{
  "code": 0,
  "identifiers": [
    {"namespace": ["example", "sales", "mart", "sales_model"], "name": "customer_mapping"}
  ]
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
NameIdentifier[] matches = client.listOntologyMappings("Sales");
```

</TabItem>
<TabItem value="python" label="Python">

```python
matches = client.list_ontology_mappings("Sales")
```

</TabItem>
</Tabs>

The SDKs return identifiers relative to the selected metalake. In both clients, `matches` contains
`sales.mart.sales_model.customer_mapping`.

## Export the Ossie Documents

Save the ontology and mapping as JSON so another application can read them.
The mapping export includes the current ontology and model names. Gravitino omits optional `iri`
file locations from the references.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -sS "$lake/ontologies/Sales/ossie?format=json" -o sales.ontology.json
curl --fail-with-body -sS "$mappings/customer_mapping/ossie?format=json" -o customer.mapping.json
```

</TabItem>
<TabItem value="java" label="Java">

```java
OssieDocument ontologyDoc = client.exportOntology("Sales", OssieFormat.JSON);
Files.writeString(Path.of("sales.ontology.json"), ontologyDoc.content());
OssieDocument mappingDoc = mappings.exportOntologyMapping("customer_mapping", OssieFormat.JSON);
Files.writeString(Path.of("customer.mapping.json"), mappingDoc.content());
```

</TabItem>
<TabItem value="python" label="Python">

```python
ontology_doc = client.export_ontology("Sales", document_format="json")
Path("sales.ontology.json").write_text(ontology_doc, encoding="utf-8")
mapping_doc = mappings.export_ontology_mapping("customer_mapping", document_format="json")
Path("customer.mapping.json").write_text(mapping_doc, encoding="utf-8")
```

</TabItem>
</Tabs>

In `customer.mapping.json`, `ontology_ref.name` is `Sales`, `semantic_model_ref.name` is
`sales_model`, and the expression is `customers.customer_id`.
Import records the document's `version` in the `ossie-version` property. Native creation defaults
this property to `0.3.0.dev0`. If you remove it, export uses the same default. Gravitino's schemas
are based on [Ossie revision `3719487`](https://github.com/apache/ossie/tree/3719487ff2744b207d0cc9bed66f5406f410dc3d/ontology).

## Remove the Sample Objects

Drop the mapping first. Gravitino rejects an attempt to drop an ontology while a mapping still uses it.

<Tabs groupId='language' queryString>
<TabItem value="shell" label="REST">

```shell
curl --fail-with-body -sS -X DELETE "$mappings/customer_mapping"
curl --fail-with-body -sS -X DELETE "$lake/ontologies/Sales"
```

</TabItem>
<TabItem value="java" label="Java">

```java
mappings.dropOntologyMapping("customer_mapping");
client.dropOntology("Sales");
```

</TabItem>
<TabItem value="python" label="Python">

```python
mappings.drop_ontology_mapping("customer_mapping")
client.drop_ontology("Sales")
```

</TabItem>
</Tabs>
