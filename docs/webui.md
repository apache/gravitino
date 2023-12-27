---
title: 'Gravitino web UI'
slug: /webui
keyword: webui
toc_min_heading_level: 2
toc_max_heading_level: 5
license: 'Copyright 2023 Datastrato Pvt Ltd. This software is licensed under the Apache License version 2.'
---

import Image from '@theme/IdealImage'
import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

This document primarily outlines how users can manage metadata within Gravitino using the web UI, the graphical interface is accessible through a web browser as an alterative to writing code or using the REST interface.

Currently, you can integrate [OAuth settings](security.md) to view, add, modify, and delete metalakes, create catalogs, and view catalogs, schemas, and tables, among other functions.

:::caution
More features are under development. For the details, please refer to the [Manage metadata using Gravitino](manage-metadata-using-gravitino.md) document.
:::

[Build](how-to-build.md#quick-start) and [deploy](getting-started.md#getting-started-locally) the Gravitino Web UI and open it in a browser at `http://<gravitino-host>:<gravitino-port>`, by default is [http://localhost:8090](http://localhost:8090).

## Initial page

The web UI homepage displayed in Gravitino depends on the configuration parameter for OAuth mode, see the details in [Security](security.md).

Set parameter for `gravitino.authenticator`, [`simple`](#simple-mode) or [`oauth`](#oauth-mode).

:::tip
After changing the configuration, make sure to restart the Gravitino server.

`<path-to-gravitino>/bin/gravitino.sh restart`
:::

### Simple mode

```
gravitino.authenticator = simple
```

Set the configuration parameter `gravitino.authenticator` to `simple`, the web UI displays the homepage (Metalakes).

![webui-metalakes-simple](assets/webui/metalakes-simple.png)

At the top-right, the UI displays the current Gravitino version.

The main content displays the existing metalake list.

### Oauth mode

```
gravitino.authenticator = oauth
```

Set the configuration parameter `gravitino.authenticator` to `oauth`, the web UI displays the login page.

![webui-login-with-oauth](assets/webui/login-with-oauth.png)

1. Enter the values corresponding to your specific configuration. For detailed instructions, please refer to [Security](security.md).

2. Clicking on the `LOGIN` button takes you to the homepage.

![webui-metalakes-oauth](assets/webui/metalakes-oauth.png)

At the top-right, there is an icon button that takes you to the login page when clicked.

## Manage metadata

> All the manage actions are performed by using the [REST API](api/rest/gravitino-rest-api)

### Metalake

#### [Create a metalake](getting-started.md#using-rest-to-interact-with-gravitino)

On the homepage, clicking on the `CREATE METALAKE` button displays a dialog to create a metalake.

![create-metalake-dialog](assets/webui/create-metalake-dialog.png)

Creating a metalake needs these fields:

1. **Name**(**_required_**): the name of the metalake.
2. **Comment**(_optional_): the comment of the metalake.
3. **Properties**(_optional_): clicking on the `ADD PROPERTY` button to add custom properties.

![metalake-list](assets/webui/metalake-list.png)

There are 3 actions you can perform on a metalake.

![metalake-actions](assets/webui/metalake-actions.png)

#### Show metalake details

![metalake-details](assets/webui/metalake-details.png)

#### Edit metalake

Displays a dialog for for modifying a metalakes fields.

![create-metalake-dialog](assets/webui/create-metalake-dialog.png)

#### Delete metalake

Displays a confirmation dialog, clicking on the `SUBMIT` button deletes this metalake.

![delete-metalake](assets/webui/delete-metalake.png)

### Catalog

Clicking on a metalake name in the table views catalogs in a metalake.

If this is the first time, it shows no data until after creating a catalog.

![metalake-catalogs](assets/webui/metalake-catalogs.png)

Clicking on the Tab - `DETAILS` views the details of the catalog in the metalake catalogs page.

![metalake-catalogs-details](assets/webui/metalake-catalogs-details.png)

#### Create a catalog

Clicking on the `CREATE CATALOG` button displays a dialog to create a catalog.

![create-catalog](assets/webui/create-catalog.png)

Creating a catalog requires these fields:

1. **Catalog name**(**_required_**): the name of the catalog
2. **Type**(**_required_**): the default value is `relational`
3. **Provider**(**_required_**): `hive`/`iceberg`/`mysql`/`postgresql`
4. **Comment**(_optional_): the comment of this catalog
5. **Properties**(**specific provider included required fields**)

##### Providers

> Required properties in various providers

<Tabs>
  <TabItem value='hive' label='Hive'>
    Follow the [Apache Hive catalog](apache-hive-catalog) document

    <Image img={require('./assets/webui/props-hive.png')} style={{ width: 480 }} />

    |Key           |Description                                           |
    |--------------|------------------------------------------------------|
    |metastore.uris|The Hive metastore URIs e.g. `thrift://127.0.0.1:9083`|

  </TabItem>
  <TabItem value='iceberg' label='Iceberg'>
    Follow the [Lakehouse Iceberg catalog](lakehouse-iceberg-catalog) document

    the parameter `catalog-backend` provides two values: `hive`, and `jdbc`.

    |Key            |Description      |
    |---------------|-----------------|
    |catalog-backend|`hive`, or `jdbc`|

    - `hive`

    <Image img={require('./assets/webui/props-iceberg-hive.png')} style={{ width: 480 }} />

    |Key      |Description                     |
    |---------|--------------------------------|
    |uri      |Iceberg catalog URI config      |
    |warehouse|Iceberg catalog warehouse config|

    - `jdbc`

    <Image img={require('./assets/webui/props-iceberg-jdbc.png')} style={{ width: 480 }} />

    |Key          |Description                                                                                            |
    |-------------|-------------------------------------------------------------------------------------------------------|
    |uri          |Iceberg catalog URI config                                                                             |
    |warehouse    |Iceberg catalog warehouse config                                                                       |
    |jdbc-driver  |"com.mysql.jdbc.Driver" or "com.mysql.cj.jdbc.Driver" for MySQL, "org.postgresql.Driver" for PostgreSQL|
    |jdbc-user    |jdbc username                                                                                          |
    |jdbc-password|jdbc password                                                                                          |

  </TabItem>
  <TabItem value='mysql' label='MySQL'>
    Follow the [JDBC MySQL catalog](jdbc-mysql-catalog) document

    <Image img={require('./assets/webui/props-mysql.png')} style={{ width: 480 }} />

    |Key          |Description                                                                                        |
    |-------------|---------------------------------------------------------------------------------------------------|
    |jdbc-driver  |JDBC URL for connecting to the database. e.g. `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver`|
    |jdbc-url     |e.g. `jdbc:mysql://localhost:3306`                                                                 |
    |jdbc-user    |The JDBC user name                                                                                 |
    |jdbc-password|The JDBC password                                                                                  |

  </TabItem>
  <TabItem value='postgresql' label='PostgreSQL'>
    Follow the [JDBC PostgreSQL catalog](jdbc-postgresql-catalog) document

    <Image img={require('./assets/webui/props-pg.png')} style={{ width: 480 }} />

    |Key          |Description                                          |
    |-------------|-----------------------------------------------------|
    |jdbc-driver  |e.g. `e.g. org.postgresql.Driver`                    |
    |jdbc-url     |e.g. `jdbc:postgresql://localhost:5432/your_database`|
    |jdbc-user    |The JDBC user name                                   |
    |jdbc-password|The JDBC password                                    |
    |jdbc-database|e.g. `pg_database`                                   |

  </TabItem>
</Tabs>

After verifying the values of these fields, clicking on the `CREATE` button creates a catalog.

![created-catalog](assets/webui/created-catalog.png)

Clicking on the icon button - `←(left arrow)` takes you to the metalake page.

### Schema

Under Construction...

- [x] View
- [ ] Create
- [ ] Edit
- [ ] Delete

Design draft preview:

![demo-tables](assets/webui/demo-tables.png)

### Table

Under Construction...

- [x] View
- [ ] Create
- [ ] Edit
- [ ] Delete

Design draft preview:

![demo-columns](assets/webui/demo-columns.png)

## Feature capabilities

| Page     | Capabilities                                                                      |
| -------- | --------------------------------------------------------------------------------- |
| Metalake | _`View`_ &#10004; / _`Create`_ &#10004; / _`Edit`_ &#10004; / _`Delete`_ &#10004; |
| Catalog  | _`View`_ &#10004; / _`Create`_ &#10004; / _`Edit`_ &#10008; / _`Delete`_ &#10008; |
| Schema   | _`View`_ &#10004; / _`Create`_ &#10008; / _`Edit`_ &#10008; / _`Delete`_ &#10008; |
| Table    | _`View`_ &#10004; / _`Create`_ &#10008; / _`Edit`_ &#10008; / _`Delete`_ &#10008; |
