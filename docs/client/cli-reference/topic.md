---
title: 'Topic commands'
slug: /cli-topic
license: 'This software is licensed under the Apache License version 2.'
---

## Topic commands 

### List all topics

Note that under a kafka messaging catalog, there is a single default *schema* named `default`.

```bash
gcli.sh topic list --name kafka.default
```

### Create a topic

```bash
gcli.sh topic create --name kafka.default.topic3
```

### Display a topic's details

```bash
gcli.sh topic details --name kafka.default.topic3
```

### Delete a topic

```bash
gcli.sh topic delete --name kafka.default.topic3
```

### Change a topic's comment

```bash
gcli.sh topic update --name kafka.default.topic3 --comment new_comment
```

## Topic properties

### Display a topic's properties

```bash
gcli.sh topic properties --name kafka.default.topic3
```

### Set a topic's property

```bash
gcli.sh topic set --name kafka.default.topic3 --property test --value value
```

### Remove a topic's property

```bash
gcli.sh topic remove --name kafka.default.topic3 --property test
```

