---
title: 'Tag commands'
slug: /cli-tag
license: 'This software is licensed under the Apache License version 2.'
---

## Tag commands 

### List all tags

```bash
gcli tag list
```

### Create tags

```bash
gcli tag create --tag tagA tagB
```

### Display a tag's details

```bash
gcli tag details --tag tagA
```

### Delete tags

```bash
gcli tag delete --tag tagA tagB
```

### Rename a tag

```bash
gcli tag update --tag tagA --rename newTag
```

### Update a tag's comment

```bash
gcli tag update --tag tagA --comment "new comment"
```


## Manage tags on an entity

### List all tags on an entity

```bash
gcli tag list --name catalog_postgres.hr
```

### Add tags to an entity

```bash
gcli tag set --name catalog_postgres.hr --tag tagA tagB
```

### Remove tags from an entity

```bash
gcli tag remove --name catalog_postgres.hr --tag tagA tagB
```

### Remove all tags from an entity

```bash
gcli tag remove --name catalog_postgres.hr
```

## Tag properties

### List the properties of a tag

```bash
gcli tag properties --tag tagA
```

### Set a properties of a tag

```bash
gcli tag set --tag tagA --property test --value value
```

### Delete a property of a tag

```bash
gcli tag remove --tag tagA --property test
```

