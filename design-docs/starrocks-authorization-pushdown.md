<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design: StarRocks Authorization Pushdown

## 1. Problem Statement and Goals

### 1.1 Problem

Apache Gravitino supports metadata management for StarRocks through the `jdbc-starrocks`
catalog, but it does not yet push Gravitino authorization changes into StarRocks native
authorization. When users create roles, grant privileges or assign roles to users or groups, 
StarRocks does not automatically receive the corresponding users, roles, grants, revokes, 
or ownership changes.

This creates a gap between the Gravitino authorization model and the access control that
StarRocks enforces when users query StarRocks directly. StarRocks already has a SQL-based
authorization model with users, roles, catalog privileges, database privileges, and table
privileges. Gravitino should translate supported Gravitino authorization operations into
StarRocks SQL statements.

### 1.2 Goals

1. Implement a StarRocks authorization pushdown plugin based on the existing JDBC
   authorization plugin framework.
2. Translate supported Gravitino securable objects and privileges into StarRocks native
   authorization objects and privileges.
3. Keep the implementation aligned with the existing authorization provider model.
4. Support user, group, role, privilege grant, privilege revoke pushdown for StarRocks catalogs.

### 1.3 Non-goals

1. This design does not add new Gravitino privilege types.
2. This design does not implement StarRocks column-level privileges.
3. This design does not implement StarRocks view, materialized view, resource, storage
   volume, system, or function privileges.
4. This design does not push down Gravitino deny privileges.
5. This design does not manage StarRocks external catalogs.

## 2. Background

### 2.1 Gravitino Authorization Pushdown

Gravitino's authorization pushdown mechanism lets a catalog load an `AuthorizationPlugin`
based on the catalog property `authorization-provider`. The provider is loaded from an
isolated package, and `BaseAuthorization.newPlugin(metalake, catalogProvider, config)`
creates a catalog-specific plugin instance.

The existing JDBC authorization base class, `JdbcAuthorizationPlugin`, already implements
the common hook flow:

1. Create or drop users.
2. Create or drop roles.
3. Grant or revoke roles to users and groups.
4. Translate Gravitino securable objects into JDBC authorization objects.
5. Generate and execute SQL statements for grants, revokes relative changes.


### 2.2 StarRocks Authorization Model

StarRocks 3.0 and later provide a role-based authorization model. The objects relevant to
this design are:

| StarRocks object | Relevant privileges                                     |
|------------------|---------------------------------------------------------|
| Catalog          | `USAGE`, `CREATE DATABASE`                              |
| Database         | `CREATE TABLE`, `ALTER`, `DROP`                         |
| Table            | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `DROP` |


### 2.3  StarRocks Catalog Mapping

The existing `jdbc-starrocks` catalog maps Gravitino metadata to StarRocks metadata as
follows:

| Gravitino object | StarRocks object                             |
|------------------|----------------------------------------------|
| Metalake         | Not represented in StarRocks                 |
| Catalog          | StarRocks internal catalog `default_catalog` |
| Schema           | Database under `default_catalog`             |
| Table            | Table under a database                       |
| Column           | Column under a table                         |


## 3. Server-side Design

### 3.1 Module Structure

```text
authorizations/authorization-starrocks/
+-- src/
    +-- main/
        +-- java/org/apache/gravitino/authorization/starrocks/
        |   +-- StarRocksAuthorization.java
        |   +-- StarRocksAuthorizationPlugin.java
        |   +-- StarRocksSecurableObject.java
        |   +-- StarRocksSecurableObjectMappingProvider.java
        |   +-- StarRocksPrivilege.java
```

### 3.2 Provider

Create a new module, `authorizations/authorization-starrocks`, with provider short name
`starrocks`. The `jdbc-starrocks` catalog enables it with:

```properties
authorization-provider=starrocks
authorization.jdbc.url=jdbc:mysql://starrocks-fe:9030/default_catalog
authorization.jdbc.username=gravitino_auth_admin
authorization.jdbc.password=secret
authorization.jdbc.driver=com.mysql.cj.jdbc.Driver
```

```java
public class StarRocksAuthorization extends BaseAuthorization<StarRocksAuthorization> {
  @Override
  public String shortName() {
    return "starrocks";
  }

  @Override
  public AuthorizationPlugin newPlugin(
      String metalake, String catalogProvider, Map<String, String> config) {
    if (!"jdbc-starrocks".equals(catalogProvider)) {
      throw new IllegalArgumentException("StarRocks authorization only supports jdbc-starrocks");
    }
    return new StarRocksAuthorizationPlugin(config);
  }
}
```


## 5. Object Mapping and Privilege Mapping

### 4.1 Securable Objects

The StarRocks mapping provider translates Gravitino securable objects into StarRocks
authorization resources.

| Gravitino object | StarRocks resource                            |
|------------------|-----------------------------------------------|
| Metalake         | `default_catalog`, all databases, all tables  |
| Catalog          | `default_catalog`, all databases, all tables  |
| Schema           | Database `{schema}`, all tables in `{schema}` |
| Table            | Table `{schema}.{table}`                      |

### 4.2 Ownership

Gravitino supports object ownership, where the owner of an object implicitly has administrative privileges such as ALTER and DROP.

Since StarRocks does not provide a native object ownership model, the plugin should map Gravitino ownership to StarRocks privileges by granting ALL privileges on the corresponding resource to the owner. 
This approach preserves the ownership semantics in Gravitino and ensures that owners retain full administrative control over their objects in StarRocks

| Gravitino object | StarRocks privilege | StarRocks resource                 |
|------------------|---------------------|------------------------------------|
| Catalog          | `ALL`               | `default_catalog`                  |
| Schema           | `ALL`               | `default_catalog.{schema}.*`       |
| Table            | `ALL`               | `default_catalog.{schema}.{table}` |

### 4.3 Supported Privileges

| Gravitino object | Gravitino privilege | StarRocks privilege                             | StarRocks resource                 |
|------------------|---------------------|-------------------------------------------------|------------------------------------|
| Catalog          | `USE_CATALOG`       | `USAGE`                                         | `default_catalog`                  |
| Catalog          | `USE_SCHEMA`        | `USAGE`                                         | `default_catalog`                  |
| Catalog          | `CREATE_SCHEMA`     | `CREATE DATABASE`                               | `default_catalog.*`                |
| Catalog          | `CREATE_TABLE`      | `CREATE TABLE`                                  | `default_catalog.*.*`              |
| Catalog          | `SELECT_TABLE`      | `SELECT`                                        | `default_catalog.*.*`              |
| Catalog          | `MODIFY_TABLE`      | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALTER` | `default_catalog.*.*`              |
| Schema           | `USE_SCHEMA`        |                                                 |                                    |
| Schema           | `CREATE_TABLE`      | `CREATE TABLE`                                  | `default_catalog.{schema}.*`       |
| Schema           | `SELECT_TABLE`      | `SELECT`                                        | `default_catalog.{schema}.*`       |
| Schema           | `MODIFY_TABLE`      | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALTER` | `default_catalog.{schema}.*`       |
| Table            | `SELECT_TABLE`      | `SELECT`                                        | `default_catalog.{schema}.{table}` |
| Table            | `MODIFY_TABLE`      | `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `ALTER` | `default_catalog.{schema}.{table}` |


### 4.4 Deny Privileges

The StarRocks pushdown plugin should only push down `Privilege.Condition.ALLOW`.
Privileges with `Privilege.Condition.DENY` should be filtered out.

### 4.5 Unsupported Privileges

The plugin should ignore or reject privileges outside the supported StarRocks table
management scope:

| Gravitino privilege family | Initial behavior                  |
|----------------------------|-----------------------------------|
| Fileset privileges         | Ignore or reject with clear error |
| Topic privileges           | Ignore or reject with clear error |
| Model privileges           | Ignore or reject with clear error |
| Function privileges        | Ignore or reject with clear error |

## 5. SQL Generation

### 5.1 User, Group, and Role Operations

The SQL generator should produce:

| Gravitino hook        | StarRocks SQL                    |
|-----------------------|----------------------------------|
| Add user              | `CREATE USER {user}`             |
| Remove user           | `DROP USER {user}`               |
| Create role           | `CREATE ROLE {role}`             |
| Drop role             | `DROP ROLE {role}`               |
| Grant role to user    | `GRANT {role} TO USER {user}`    |
| Revoke role from user | `REVOKE {role} FROM USER {user}` |


### 5.2 Privilege Grant SQL

The grant generator should emit one SQL statement per StarRocks privilege:

```sql
GRANT SELECT ON TABLE `db1`.`tbl1` TO ROLE `catalog_user`;
GRANT INSERT ON ALL TABLES IN DATABASE `db1` TO ROLE `catalog_user`;
GRANT CREATE TABLE ON DATABASE `db1` TO ROLE `catalog_user`;
GRANT CREATE DATABASE ON CATALOG `default_catalog` TO ROLE `catalog_user`;
GRANT USAGE ON CATALOG `default_catalog` TO ROLE `catalog_user`;
```

The implementation should keep the existing one-privilege-per-SQL behavior from
`JdbcAuthorizationPlugin`. This keeps duplicate-grant handling simple and avoids failing a
multi-privilege SQL statement when only one privilege is already granted.

### 5.3 Privilege Revoke SQL

The revoke generator mirrors the grant generator:

```sql
REVOKE SELECT ON TABLE `db1`.`tbl1` FROM ROLE `catalog_user`;
REVOKE INSERT ON ALL TABLES IN DATABASE `db1` FROM ROLE `catalog_user`;
REVOKE CREATE TABLE ON DATABASE `db1` FROM ROLE `catalog_user`;
REVOKE CREATE DATABASE ON CATALOG `default_catalog` FROM ROLE `catalog_user`;
REVOKE USAGE ON CATALOG `default_catalog` FROM ROLE `catalog_user`;
```

## 6. References

1. StarRocks privilege items:
   https://docs.starrocks.io/docs/administration/user_privs/authorization/privilege_item/
2. StarRocks `GRANT` syntax:
   https://docs.starrocks.io/docs/sql-reference/sql-statements/account-management/GRANT/