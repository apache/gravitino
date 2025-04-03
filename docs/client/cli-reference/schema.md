---
title: 'Schema commands'
slug: /cli-schema
license: 'This software is licensed under the Apache License version 2.'
---

## Schema commands 

### List all schemas in a catalog

```bash
gcli.sh schema list --name catalog_postgres
```

### Show the details about a schema

```bash
gcli.sh schema details --name catalog_postgres.hr
```

### Show a schema's audit information

```bash
gcli.sh schema details --name catalog_postgres.hr --audit
```

### Create a schema

```bash
gcli.sh schema create --name catalog_postgres.new_db
```

### Display schema properties

```bash
gcli.sh schema properties --name catalog_postgres.hr
```

Setting and removing schema properties is not currently supported by the Java API or the Gravitino CLI.

