---
title: 'Owner commands'
slug: /cli-owner
license: 'This software is licensed under the Apache License version 2.'
---

## Owner commands 

### List owners

```bash
gcli.sh catalog details --owner --name postgres
```

### Set a user as the owner

```bash
gcli.sh catalog set --owner --user admin --name postgres
```

### Set a group as the owner

```bash
gcli.sh catalog set --owner --group groupA --name postgres
```

