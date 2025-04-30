---
title: 'Metalake commands'
slug: /cli-metalake
license: 'This software is licensed under the Apache License version 2.'
---

## Metalake commands 

#### List all metalakes

```bash
gcli.sh metalake list
```

#### Show a metalake details

```bash
gcli.sh metalake details
```

#### Show a metalake's audit information

```bash
gcli.sh metalake details --audit
```

#### Create a metalake

```bash
gcli.sh metalake create --metalake my_metalake --comment "This is my metalake"
```

#### Delete a metalake

```bash
gcli.sh metalake delete
```

#### Rename a metalake

```bash
gcli.sh metalake update --rename demo
```

#### Update a metalake's comment

```bash
gcli.sh metalake update --comment "new comment"
```

#### Display a metalake's properties

```bash
gcli.sh metalake properties
```

#### Set a metalake's property

```bash
gcli.sh metalake set --property a_property --value a_value
```

#### Remove a metalake's property

```bash
gcli.sh metalake remove --property a_property
```

#### Enable a metalake

```bash
gcli.sh metalake update -m my_metalake --enable
```

#### Enable a metalake and all catalogs

```bash
gcli.sh metalake update -m my_metalake --enable --all
```

#### Disable a metalake

```bash
gcli.sh metalake update -m my_metalake --disable
```

