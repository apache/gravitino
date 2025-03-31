---
title: 'Owner commands'
slug: /cli-owner
license: 'This software is licensed under the Apache License version 2.'
---

## Owner commands 

### List owners

```bash
gcli catalog details --owner --name postgres
```

### Set an owner to a user

```bash
gcli catalog set --owner --user admin --name postgres
```

### Set an owner to a group

```bash
gcli catalog set --owner --group groupA --name postgres
```


