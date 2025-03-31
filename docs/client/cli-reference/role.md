---
title: 'Role commands'
slug: /cli-role
license: 'This software is licensed under the Apache License version 2.'
---

## Role commands 

When granting or revoking privileges the following privileges can be used.

```
create_catalog, use_catalog, create_schema, use_schema,
create_table, modify_table, select_table,
create_fileset, write_fileset, read_fileset,
create_topic, produce_topic, consume_topic,
manage_users, create_role, manage_grants
```

Note that some are only valid for certain entities.

### List all roles

```bash
gcli role list
```

### Create a role

```bash
gcli role create --role admin
```

### Display role details

```bash
gcli role details --role admin
```

### Show a roles's audit information

```bash
gcli role details --role admin --audit
```

### Delete a role

```bash
gcli role delete --role admin
```

## User bindings for roles

### Add a role to a user

```bash
gcli user grant --user new_user --role admin
```

### Remove a role from a user

```bash
gcli user revoke --user new_user --role admin
```

### Remove all roles from a user

```bash
gcli user revoke --user new_user --all
```

## Group bindings for roles

### Add a role to a group

```bash
gcli group grant --group groupA --role admin
```

### Remove a role from a group

```bash
gcli group revoke --group groupA --role admin
```

### Remove all roles from a group

```bash
gcli group revoke --group groupA --all
```

## Privilige operations

### Grant a privilege

```bash
gcli role grant --name catalog_postgres --role admin --privilege create_table modify_table
```

### Revoke a privilege

```bash
gcli role revoke --metalake metalake_demo --name catalog_postgres --role admin --privilege create_table modify_table
```

### Revoke all privileges

```bash
gcli role revoke --metalake metalake_demo --name catalog_postgres --role admin --all
```

