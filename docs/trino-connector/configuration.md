---
title: "Trino Connector Configuration"
slug: "/trino-connector/configuration"
keyword: "gravitino connector trino"
license: "This software is licensed under the Apache License version 2."
---

## Introduction

| Property                                    | Type    | Default Value         | Description                                                                                                                                                                                                                                                                                                         | Required |
|---------------------------------------------|---------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|
| connector.name                              | string  | (none)                | The `connector.name` defines the type of Trino connector, this value is always 'gravitino'.                                                                                                                                                                                                                         | Yes      |
| gravitino.metalake                          | string  | (none)                | The `gravitino.metalake` defines which metalake in Gravitino server the Trino connector uses. Trino connector should set it at start, the value of `gravitino.metalake` needs to be a valid name, Trino connector can detect and load the metalake with catalogs, schemas and tables once created and keep in sync. | Yes      |
| gravitino.uri                               | string  | http://localhost:8090 | The `gravitino.uri` defines the connection URL of the Gravitino server, the default value is `http://localhost:8090`. Trino connector can detect and connect to Gravitino server once it is ready, no need to start Gravitino server beforehand.                                                                    | No       |
| trino.jdbc.user                             | string  | admin                 | The jdbc user name of current Trino.                                                                                                                                                                                                                                                                                | NO       |
| trino.jdbc.password                         | string  | (none)                | The jdbc password of current Trino.                                                                                                                                                                                                                                                                                 | NO       |
| gravitino.metadata.refresh-interval-seconds | integer | 10                    | The `gravitino.metadata.refresh-interval-seconds` defines the interval in seconds to refresh metadata from Gravitino server, the default value is 10 seconds.                                                                                                                                                       | No       |
| gravitino.trino.skip-version-validation     | boolean | false                 | The `gravitino.trino.skip-version-validation` defines whether to skip Trino version validation. Gravitino supports Trino versions between 440 and 478. If this option is `true`, unsupported Trino versions can still be used, but compatibility is not guaranteed.                                                 | No       |
| gravitino.client.                           | string  | (none)                | The configuration key prefix for the Gravitino client config.                                                                                                                                                                                                                                                       | No       |
| gravitino.trino.skip-catalog-patterns       | string  | (none)                | The `gravitino.trino.skip-catalog-patterns` defines a comma-separated list of catalog name regex patterns that should be excluded from loading. For example, `test_.*, .*_tmp` excludes all catalogs starting with `test_` or ending with `_tmp`.                                                                   | No       |
| gravitino.use-single-metalake               | boolean | true                  | If `true`, only one metalake is used and catalogs are identified by `<catalog_name>`. If `false`, multi-metalake mode is enabled and catalogs are identified by `<metalake_name>.<catalog_name>`.                                                                                                                   | No       |
| gravitino.iceberg.rest-uri                  | string  | (none)                | The endpoint of the Gravitino Iceberg REST server (IRC). Discovered automatically from the Gravitino server, which is asked whether it has an IRC running for this connector's metalake; set this only to override the discovered value. When an endpoint is available (discovered or configured), `lakehouse-iceberg` catalogs whose `catalog-backend` is not `rest` are loaded through it instead of being translated into Trino's `jdbc` or `hive_metastore` Iceberg catalog type — this is what makes credential vending work. | No       |
| gravitino.iceberg.rest-catalog.             | string  | (none)                | The configuration key prefix for properties passed through to the internal Trino Iceberg REST catalog. The prefix is rewritten to `iceberg.rest-catalog.`, so `gravitino.iceberg.rest-catalog.security=OAUTH2` becomes `iceberg.rest-catalog.security=OAUTH2`. The `uri`, `warehouse` and `prefix` keys are reserved and always derived by the connector.                     | No       |

To configure the Gravitino client, use properties prefixed with `gravitino.client.`. These properties will directly passed to the Gravitino client.

**Note:** Invalid configuration properties will result in exceptions. Please see [Gravitino Java client configurations](../how-to-use-gravitino-client.md#java-client-configuration) for more support client configuration.

Upgrading an existing deployment needs no action: a Gravitino server without the Iceberg REST server
running reports no endpoint, so `lakehouse-iceberg` catalogs keep translating `catalog-backend` as
before. See [Iceberg catalog](./catalog-iceberg.md#how-trino-reaches-the-catalog).

Multi-metalake mode (`gravitino.use-single-metalake=false`) is supported on Trino connector versions 440-445 and 469-478. On versions 446-468, a warning is logged and the connector initializes, but the mode is not fully supported and some operations may fail.

**Note:** In multi-metalake mode, `gravitino.iceberg.rest-uri` is only honored when scoped to a
metalake, as `gravitino.iceberg.rest-uri.<metalake_name>` — the unscoped form is ignored, since a
single Iceberg REST server serves exactly one metalake and applying it to every metalake would
misroute the others. The unscoped form remains valid in single-metalake mode.

## Authentication

The Gravitino Trino connector supports authenticating to the Gravitino server using Simple, Basic, OAuth, and Kerberos authentication. For detailed authentication configuration, refer to [Trino Connector Authentication](./authentication.md).
