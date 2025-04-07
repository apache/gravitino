---
title: 'Fileset commands'
slug: /cli-fileset
license: 'This software is licensed under the Apache License version 2.'
---

## Fileset  commands 

### List filesets

```bash
gcli.sh fileset list --name hadoop.schema
```

### Create a fileset

```bash
gcli.sh fileset create --name hadoop.schema.fileset --properties managed=true,location=file:/tmp/root/schema/example
```

### Display a fileset's details

```bash
gcli.sh fileset details --name hadoop.schema.fileset
```

### Delete a fileset

```bash
gcli.sh fileset delete --name hadoop.schema.fileset
```

### Update a fileset's comment

```bash
gcli.sh fileset update --name hadoop.schema.fileset --comment new_comment
```

### Rename a fileset

```bash
gcli.sh fileset update --name hadoop.schema.fileset --rename new_name
```

## Fileset properties

### List a fileset's properties

```bash
gcli.sh fileset properties --name hadoop.schema.fileset 
```

### Set a fileset's property

```bash
gcli.sh fileset set  --name hadoop.schema.fileset --property test --value value
```

### Remove a fileset's property

```bash
gcli.sh fileset remove --name hadoop.schema.fileset --property test
```


