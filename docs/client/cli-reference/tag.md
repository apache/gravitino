---
title: 'Tag commands'
slug: /cli-tag
license: 'This software is licensed under the Apache License version 2.'
---

## Tag commands 

### List all tags

```bash
gcli.sh tag list
```

### Create a tag or some tags

```bash
gcli.sh tag create --tag tagA tagB
```

### Display a tag's details

```bash
gcli.sh tag details --tag tagA
```

### Delete one or more tags

```bash
gcli.sh tag delete --tag tagA tagB
```

### Rename a tag

```bash
gcli.sh tag update --tag tagA --rename newTag
```

### Update a tag's comment

```bash
gcli.sh tag update --tag tagA --comment "new comment"
```

## Manage tags on an entity

### List all tags on an entity

```bash
gcli.sh tag list --name catalog_postgres.hr
```

### Add/set tags to an entity

```bash
gcli.sh tag set --name catalog_postgres.hr --tag tagA tagB
```

### Remove tags from an entity

```bash
gcli.sh tag remove --name catalog_postgres.hr --tag tagA tagB
```

### Remove all tags from an entity

```bash
gcli.sh tag remove --name catalog_postgres.hr
```

## Tag properties

### List the properties of a tag

```bash
gcli.sh tag properties --tag tagA
```

### Set a properties of a tag

```bash
gcli.sh tag set --tag tagA --property test --value value
```

### Delete a property of a tag

```bash
gcli.sh tag remove --tag tagA --property test
```

