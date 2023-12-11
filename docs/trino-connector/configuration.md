---
title: "Gravitino connector Configuration"
slug: /trino-connector/configuration
keyword: gravition connector trino
license: Copyright 2023 Datastrato Pvt. This software is licensed under the Apache License version 2.
---
### Gravitino connector properties

| Property          | Type   | Default                | Description                                                                                                                                                                                                                                                                                                                                                                           |
| ----------------- | ------ |------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| gravitino.url     | string | http://localhost:8090  | The `gravitino.url` defines the connection URL for the Gravitino server. If not set, the default value is `http://localhost:8090`. If the Gravitino server is not starting, Trino can still start normally. Once the Gravitino server is up and running, the Gravitino connector will automatically connect. You can find error details in the Trino logs.                            |
| gravitino.metalake| string | ""                     | The `gravitino.metalake` defines the metalake used. You can create it beforehand or later on. If not set, Trino might throw an error upon startup. It must be set to a valid metalake name. If it's not created, the Gravitino connector continues checking until it's created. Once created, it will load the catalogs, schemas, and tables into Trino and maintain synchronization. |