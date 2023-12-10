---
title: "Gravitino connector Configuration"
slug: /trino-connector/configuration
keyword: gravition connector trino
license: Copyright 2023 Datastrato Pvt. This software is licensed under the Apache License version 2.
---
### Gravitino connector properties

# gravitino.url
- type: string
- default: http://localhost:8090

The gravitino.url defines the connection url about Gravitino server. If not set, the default value is http://localhost:8090.
If the Gravitino server is not started, Trino can still start normally. Once the Gravitino server is up and running, 
the Gravitino connector will automatically connect. More error details can be found in the Trino logs.
 

# gravitino.metalake
- type: string
- default: ""

The gravitino.metalake defines which metalake are used. It can be created beforehand or later on. 
If not set, Trino might throw an error upon startup. 
If it's not created, the Gravitino connector will continue checking until it's created. 
Once created, it will load the catalogs, schemas, and tables into Trino and maintain synchronization.