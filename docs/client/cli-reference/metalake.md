---
title: 'Metalake commands'
slug: /cli-metalake
license: 'This software is licensed under the Apache License version 2.'
---

## Metalake commands 

#### Show all metalakes

```bash
gcli metalake list
```

#### Show a metalake details

```bash
gcli metalake details
```

#### Show a metalake's audit information

```bash
gcli metalake details --audit
```

#### Create a metalake

```bash
gcli metalake create --metalake my_metalake --comment "This is my metalake"
```

#### Delete a metalake

```bash
gcli metalake delete
```

#### Rename a metalake

```bash
gcli metalake update --rename demo
```

#### Update a metalake's comment

```bash
gcli metalake update --comment "new comment"
```

#### Display a metalake's properties

```bash
gcli metalake properties
```

#### Set a metalake's property

```bash
gcli metalake set --property test --value value
```

#### Remove a metalake's property

```bash
gcli metalake remove --property test
```

#### Enable a metalake

```bash
gcli metalake update -m metalake_demo --enable
```

#### Enable a metalake and all catalogs

```bash
gcli metalake update -m metalake_demo --enable --all
```

#### Disable a metalake

```bash
gcli metalake update -m metalake_demo --disable
```

