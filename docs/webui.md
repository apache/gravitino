---
title: 'Apache Gravitino web UI'
slug: /webui
keyword: webui
last_update:
  date: 2024-10-30
  author: LauraXia123
license: 'This software is licensed under the Apache License version 2.'
---

This document primarily outlines how users can manage metadata within Apache Gravitino using the web UI, the graphical interface is accessible through a web browser as an alternative to writing code or using the REST interface.

Currently, you can integrate [OAuth settings](security/security.md) to view, add, modify, and delete metalakes, create catalogs, and view catalogs, schemas, and tables, among other functions.

[Build](./how-to-build.md#quick-start) and [deploy](./getting-started.md#getting-started-locally) the Gravitino Web UI and open it in a browser at `http://<gravitino-host>:<gravitino-port>`, by default is [http://localhost:8090](http://localhost:8090).

## Initial page

The web UI homepage displayed in Gravitino depends on the configuration parameter for OAuth mode, see the details in [Security](security/security.md).

Set parameter for `gravitino.authenticators`, [`simple`](#simple-mode) or [`oauth`](#oauth-mode). Simple mode is the default authentication option. If multiple authenticators are set, the first one is taken by default.

:::tip
After changing the configuration, make sure to restart the Gravitino server.

`<path-to-gravitino>/bin/gravitino.sh restart`
:::

### Simple mode

```text
gravitino.authenticators = simple
```

Set the configuration parameter `gravitino.authenticators` to `simple`, and the web UI displays the homepage (Metalakes).

![webui-metalakes-simple](./assets/webui/metalakes-simple.png)

At the top-right, the UI displays the current Gravitino version.

The main content displays the existing metalake list.

### Oauth mode

```text
gravitino.authenticators = oauth
```

Set the configuration parameter `gravitino.authenticators` to `oauth`, and the web UI displays the login page.

:::caution
If both `OAuth` and `HTTPS` are set, due to the different security permission rules of various browsers, to avoid cross-domain errors,
it is recommended to use the [Chrome](https://www.google.com/chrome/) browser for access and operation.

Such as Safari need to enable the developer menu, and select `Disable Cross-Origin Restrictions` from the develop menu.
:::

![webui-login-with-oauth](./assets/webui/login-with-oauth.png)

1. Enter the values corresponding to your specific configuration. For detailed instructions, please refer to [Security](security/security.md).

2. Click on the `LOGIN` button takes you to the homepage.

![webui-metalakes-oauth](./assets/webui/metalakes-oauth.png)

At the top-right, there is an icon button that takes you to the login page when clicked.

## Manage metadata

> All the manage actions are performed by using the [REST API](api/rest/gravitino-rest-api)

### Metalake

#### [Create metalake](./getting-started.md#using-rest-to-interact-with-gravitino)

On the homepage, clicking on the `CREATE METALAKE` button displays a dialog to create a metalake.

![create-metalake-dialog](./assets/webui/create-metalake-dialog.png)

Creating a metalake needs these fields:

1. **Name**(**_required_**): the name of the metalake.
2. **Comment**(_optional_): the comment of the metalake.
3. **Properties**(_optional_): Click on the `ADD PROPERTY` button to add custom properties.

![metalake-list](./assets/webui/metalake-list.png)

There are 3 actions you can perform on a metalake.

![metalake-actions](./assets/webui/metalake-actions.png)

#### Show metalake details

Click on the action icon <Icon icon='bx:show-alt' fontSize='24' /> in the table cell.

You can see the detailed information of this metalake in the drawer component on the right.

![metalake-details](./assets/webui/metalake-details.png)

#### Edit metalake

Click on the action icon <Icon icon='mdi:square-edit-outline' fontSize='24' /> in the table cell.

Displays the dialog for modifying fields of the selected metalake.

![create-metalake-dialog](./assets/webui/create-metalake-dialog.png)

#### Disable metalake

Metalake defaults to in-use after successful creation.

Mouse over the switch next to the metalake's name to see the 'In-use' tip.

![metalake-in-use](./assets/webui/metalake-in-use.png)

Click on the switch will disable the metalake, mouse over the switch next to the metalake's name to see the 'Not in-use' tip.

![metalake-not-in-use](./assets/webui/metalake-not-in-use.png)

#### Drop metalake

Click on the action icon <Icon icon='mdi:delete-outline' fontSize='24' color='red' /> in the table cell.

Displays a confirmation dialog, clicking on the `DROP` button drops this metalake.

![delete-metalake](./assets/webui/delete-metalake.png)

### Catalog

Click on a metalake name in the table views catalogs in a metalake.

If this is the first time, it shows no data until after creating a catalog.

Click on the left arrow icon button <Icon icon='mdi:arrow-left' fontSize='24' color='#6877ef' /> takes you to the metalake page.

![metalake-catalogs](./assets/webui/metalake-catalogs.png)

Click on the Tab - `DETAILS` views the details of the metalake on the metalake catalogs page.

![metalake-catalogs-details](./assets/webui/metalake-catalogs-details.png)

On the left side of the page is a tree list, and the icons of the catalog correspond to their type and provider.

- Catalog <Icon icon='openmoji:iceberg' fontSize='24px' /> (e.g. iceberg catalog)
- Schema <Icon icon='bx:coin-stack' fontSize='24px' />
- Table <Icon icon='bx:table' fontSize='24px' />

![tree-view](./assets/webui/tree-view.png)

Hover your mouse over the corresponding icon to the data changes to a reload icon <Icon icon='mdi:reload' fontSize='24px' />. Click on this icon to reload the currently selected data.

![tree-view-reload-catalog](./assets/webui/tree-view-reload-catalog.png)

#### Create catalog

Click on the `CREATE CATALOG` button displays the dialog to create a catalog.

![create-catalog](./assets/webui/create-catalog.png)

Creating a catalog requires these fields:

1. **Catalog name**(**_required_**): the name of the catalog
2. **Type**(**_required_**): `relational`/`fileset`/`messaging`/`model`, the default value is `relational`
3. **Provider**(**_required_**):
    1. Type `relational` - `hive`/`iceberg`/`mysql`/`postgresql`/`doris`/`paimon`/`hudi`/`oceanbase`
    2. Type `fileset` - `hadoop`
    3. Type `messaging` - `kafka`
    4. Type `model` has no provider
4. **Comment**(_optional_): the comment of this catalog
5. **Properties**(**each `provider` must fill in the required property fields specifically**)

##### Providers

> Required properties in various providers

###### 1. Type `relational`

<Tabs>
  <TabItem value='hive' label='Hive'>
    Follow the [Apache Hive catalog](./apache-hive-catalog.md) document.

    <Image img={require('./assets/webui/props-hive.png')} style={{ width: 480 }} />

    |Key           |Description                                           |
    |--------------|------------------------------------------------------|
    |metastore.uris|The Hive metastore URIs e.g. `thrift://127.0.0.1:9083`|

  </TabItem>
  <TabItem value='iceberg' label='Iceberg'>
    Follow the [Lakehouse Iceberg catalog](./lakehouse-iceberg-catalog.md) document.

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

    the parameter `authentication.type` provides two values: `simple`, and `Kerberos`.

    <Image img={require('./assets/webui/props-authentication-type.png')} style={{ width: 480 }} />

    - `Kerberos`

    <Image img={require('./assets/webui/props-authentication-kerberos.png')} style={{ width: 480 }} />

    |Key                               |Description                                                                                                  |
    |----------------------------------|-------------------------------------------------------------------------------------------------------------|
    |authentication.type               |The type of authentication for Paimon catalog backend, currently Gravitino only supports Kerberos and simple.|
    |authentication.kerberos.principal |The principal of the Kerberos authentication.                                                                |
    |authentication.kerberos.keytab-uri|The URI of The keytab for the Kerberos authentication.                                                       |

  </TabItem>
  <TabItem value='mysql' label='MySQL'>
    Follow the [JDBC MySQL catalog](./jdbc-mysql-catalog.md) document.

    <Image img={require('./assets/webui/props-mysql.png')} style={{ width: 480 }} />

    |Key          |Description                                                                                        |
    |-------------|---------------------------------------------------------------------------------------------------|
    |jdbc-driver  |JDBC URL for connecting to the database. e.g. `com.mysql.jdbc.Driver` or `com.mysql.cj.jdbc.Driver`|
    |jdbc-url     |e.g. `jdbc:mysql://localhost:3306`                                                                 |
    |jdbc-user    |The JDBC user name                                                                                 |
    |jdbc-password|The JDBC password                                                                                  |

  </TabItem>
  <TabItem value='postgresql' label='PostgreSQL'>
    Follow the [JDBC PostgreSQL catalog](./jdbc-postgresql-catalog) document.

    <Image img={require('./assets/webui/props-pg.png')} style={{ width: 480 }} />

    |Key          |Description                                          |
    |-------------|-----------------------------------------------------|
    |jdbc-driver  |e.g. `org.postgresql.Driver`                         |
    |jdbc-url     |e.g. `jdbc:postgresql://localhost:5432/your_database`|
    |jdbc-user    |The JDBC user name                                   |
    |jdbc-password|The JDBC password                                    |
    |jdbc-database|e.g. `pg_database`                                   |

  </TabItem>
  <TabItem value='doris' label='Doris'>
    Follow the [JDBC Doris catalog](./jdbc-doris-catalog.md) document.

    <Image img={require('./assets/webui/props-doris.png')} style={{ width: 480 }} />

    |Key          |Description                                                          |
    |-------------|---------------------------------------------------------------------|
    |jdbc-driver  |JDBC URL for connecting to the database. e.g. `com.mysql.jdbc.Driver`|
    |jdbc-url     |e.g. `jdbc:mysql://localhost:9030`                                   |
    |jdbc-user    |The JDBC user name                                                   |
    |jdbc-password|The JDBC password                                                    |

  </TabItem>
  <TabItem value='Paimon' label='Paimon'>
    Follow the [lakehouse-paimon-catalog](./lakehouse-paimon-catalog.md) document.

    the parameter `catalog-backend` provides three values: `filesystem`, `hive`, and `jdbc`.

    |Key            |Description                    |
    |---------------|-------------------------------|
    |catalog-backend|`filesystem`, `hive`, or `jdbc`|

    - `filesystem`

    <Image img={require('./assets/webui/props-paimon-filesystem.png')} style={{ width: 480 }} />

    |Key      |Description                     |
    |---------|--------------------------------|
    |warehouse|Paimon catalog warehouse config |

    - `hive`

    <Image img={require('./assets/webui/props-paimon-hive.png')} style={{ width: 480 }} />

    |Key      |Description                     |
    |---------|--------------------------------|
    |uri      |Paimon catalog URI config       |
    |warehouse|Paimon catalog warehouse config |

    - `jdbc`

    <Image img={require('./assets/webui/props-paimon-jdbc.png')} style={{ width: 480 }} />

    |Key          |Description                                                                                            |
    |-------------|-------------------------------------------------------------------------------------------------------|
    |uri          |Paimon catalog URI config                                                                              |
    |warehouse    |Paimon catalog warehouse config                                                                        |
    |jdbc-driver  |"com.mysql.jdbc.Driver" or "com.mysql.cj.jdbc.Driver" for MySQL, "org.postgresql.Driver" for PostgreSQL|
    |jdbc-user    |jdbc username                                                                                          |
    |jdbc-password|jdbc password                                                                                          |
  
    the parameter `authentication.type` provides two values: `simple`, and `Kerberos`.

    <Image img={require('./assets/webui/props-authentication-type.png')} style={{ width: 480 }} />

    - `Kerberos`

    <Image img={require('./assets/webui/props-authentication-kerberos.png')} style={{ width: 480 }} />


    |Key                               |Description                                                                                                  |
    |----------------------------------|-------------------------------------------------------------------------------------------------------------|
    |authentication.type               |The type of authentication for Paimon catalog backend, currently Gravitino only supports Kerberos and simple.|
    |authentication.kerberos.principal |The principal of the Kerberos authentication.                                                                |
    |authentication.kerberos.keytab-uri|The URI of The keytab for the Kerberos authentication.                                                       |

  </TabItem>
  <TabItem value='Hudi' label='Hudi'>
    Follow the [lakehouse-hudi-catalog](./lakehouse-hudi-catalog.md) document.

    <Image img={require('./assets/webui/props-hudi.png')} style={{ width: 480 }} />

    |Key            |Description                    |
    |---------------|-------------------------------|
    |catalog-backend|`hms`                          |
    |uri            |Hudi catalog URI config        |

  </TabItem>
  <TabItem value='OceanBase' label='OceanBase'>
    Follow the [jdbc-oceanbase-catalog](./jdbc-oceanbase-catalog.md) document.

    <Image img={require('./assets/webui/props-oceanbase.png')} style={{ width: 480 }} />

    |Key          |Description                                                                        |
    |-------------|-----------------------------------------------------------------------------------|
    |jdbc-driver  |e.g. com.mysql.jdbc.Driver or com.mysql.cj.jdbc.Driver or com.oceanbase.jdbc.Driver|
    |jdbc-url     |e.g. jdbc:mysql://localhost:2881 or jdbc:oceanbase://localhost:2881                |
    |jdbc-user    |The JDBC user name                                                                 |
    |jdbc-password|The JDBC password                                                                  |

  </TabItem>
</Tabs>

###### 2. Type `fileset`

<Tabs>
  <TabItem value='hadoop' label='Hadoop'>
    Follow the [Hadoop catalog](./hadoop-catalog.md) document.

    <Image img={require('./assets/webui/create-fileset-hadoop-catalog-dialog.png')} style={{ width: 480 }} />

  </TabItem>
</Tabs>

###### 3. Type `messaging`

<Tabs>
  <TabItem value='kafka' label='Kafka'>
    Follow the [Kafka catalog](./kafka-catalog.md) document.

    <Image img={require('./assets/webui/create-messaging-kafka-catalog-dialog.png')} style={{ width: 480 }} />

    | Key               | Description                                                                               |
    | ----------------- | ----------------------------------------------------------------------------------------- |
    | bootstrap.servers | The Kafka broker(s) to connect to, allowing for multiple brokers by comma-separating them |

  </TabItem>
</Tabs>

After verifying the values of these fields, clicking on the `CREATE` button creates a catalog.

![created-catalog](./assets/webui/created-catalog.png)

#### Show catalog details

Click on the action icon <Icon icon='bx:show-alt' fontSize='24' /> in the table cell.

You can see the detailed information of this catalog in the drawer component on the right.

![show-catalog-details](./assets/webui/show-catalog-details.png)

#### Edit catalog

Click on the action icon <Icon icon='mdi:square-edit-outline' fontSize='24' /> in the table cell.

Displays the dialog for modifying fields of the selected catalog.

![update-catalog](./assets/webui/update-catalog.png)

Only the `name`, `comment`, and custom fields in `properties` can be modified, other fields such as `type`, `provider`, and default fields in `properties` cannot be modified.

The fields that are not allowed to be modified cannot be selected and modified in the web UI.

#### Disable catalog

Catalog defaults to in-use after successful creation.

Mouse over the switch next to the catalog's name to see the 'In-use' tip.

![catalog-in-use](./assets/webui/catalog-in-use.png)

Click on the switch will disable the catalog, mouse over the switch next to the catalog's name to see the 'Not in-use' tip.

![catalog-not-in-use](./assets/webui/catalog-not-in-use.png)

#### Delete catalog

Click on the action icon <Icon icon='mdi:delete-outline' fontSize='24' color='red' /> in the table cell.

Displays a confirmation dialog, clicking on the SUBMIT button deletes this catalog.

![delete-catalog](./assets/webui/delete-catalog.png)

### Schema

Click the catalog tree node on the left sidebar or the catalog name link in the table cell.

Displays the list schemas of the catalog.

![list-schemas](./assets/webui/list-schemas.png)

#### Create schema

Click on the `CREATE SCHEMA` button displays the dialog to create a schema.

![create-schema](./assets/webui/create-schema.png)

Creating a schema needs these fields:

1. **Name**(**_required_**): the name of the schema.
2. **Comment**(_optional_): the comment of the schema.
3. **Properties**(_optional_): Click on the `ADD PROPERTY` button to add custom properties.

#### Show schema details

Click on the action icon <Icon icon='bx:show-alt' fontSize='24' /> in the table cell.

You can see the detailed information of this schema in the drawer component on the right.

![schema-details](./assets/webui/schema-details.png)

#### Edit schema

Click on the action icon <Icon icon='mdi:square-edit-outline' fontSize='24' /> in the table cell.

Displays the dialog for modifying fields of the selected schema.

![update-schema-dialog](./assets/webui/update-schema-dialog.png)

#### Drop schema

Click on the action icon <Icon icon='mdi:delete-outline' fontSize='24' color='red' /> in the table cell.

Displays a confirmation dialog, clicking on the `DROP` button drops this schema.

![delete-schema](./assets/webui/delete-schema.png)

### Table

Click the hive schema tree node on the left sidebar or the schema name link in the table cell.

Displays the list tables of the schema.

![list-tables](./assets/webui/list-tabels.png)

#### Create table

Click on the `CREATE TABLE` button displays the dialog to create a table.

![create-table](./assets/webui/create-table.png)

Creating a table needs these fields:

1. **Name**(**_required_**): the name of the table.
2. **columns**(**_required_**): 
    1. The name and type of each column are required.
    2. Only suppport simple types, cannot support complex types by ui, you can create complex types by api.
3. **Comment**(_optional_): the comment of the table.
4. **Properties**(_optional_): Click on the `ADD PROPERTY` button to add custom properties.

#### Show table details

Click on the action icon <Icon icon='bx:show-alt' fontSize='24' /> in the table cell.

You can see the detailed information of this table in the drawer component on the right.

![table-details](./assets/webui/table-details.png)

Click the table tree node on the left sidebar or the table name link in the table cell.

You can see the columns and detailed information on the right page.

![list-columns](./assets/webui/list-columns.png)
![table-selected-details](./assets/webui/table-selected-details.png)

#### Edit table

Click on the action icon <Icon icon='mdi:square-edit-outline' fontSize='24' /> in the table cell.

Displays the dialog for modifying fields of the selected table.

![update-table-dialog](./assets/webui/update-table-dialog.png)

#### Drop table

Click on the action icon <Icon icon='mdi:delete-outline' fontSize='24' color='red' /> in the table cell.

Displays a confirmation dialog, clicking on the `DROP` button drops this table.

![delete-table](./assets/webui/delete-table.png)

### Fileset

Click the fileset schema tree node on the left sidebar or the schema name link in the table cell.

Displays the list filesets of the schema.

![list-filesets](./assets/webui/list-filesets.png)

#### Create fileset

Click on the `CREATE FILESET` button displays the dialog to create a fileset.

![create-fileset](./assets/webui/create-fileset.png)

Creating a fileset needs these fields:

1. **Name**(**_required_**): the name of the fileset.
2. **Type**(**_required_**): `managed`/`external`, the default value is `managed`.
3. **Storage Location**(_optional_): 
    1. It is optional if the fileset is 'Managed' type and a storage location is already specified at the parent catalog or schema level.
    2. It becomes mandatory if the fileset type is 'External' or no storage location is defined at the parent level.
4. **Comment**(_optional_): the comment of the fileset.
5. **Properties**(_optional_): Click on the `ADD PROPERTY` button to add custom properties.

#### Show fileset details

Click on the action icon <Icon icon='bx:show-alt' fontSize='24' /> in the table cell.

You can see the detailed information of this fileset in the drawer component on the right.

![fileset-details](./assets/webui/fileset-details.png)

Click the fileset tree node on the left sidebar or the fileset name link in the table cell.

You can see the detailed information on the right page.

![fileset-selected-details](./assets/webui/fileset-selected-details.png)

#### Edit fileset

Click on the action icon <Icon icon='mdi:square-edit-outline' fontSize='24' /> in the table cell.

Displays the dialog for modifying fields of the selected fileset.

![update-fileset-dialog](./assets/webui/update-fileset-dialog.png)

#### Drop fileset

Click on the action icon <Icon icon='mdi:delete-outline' fontSize='24' color='red' /> in the table cell.

Displays a confirmation dialog, clicking on the `DROP` button drops this fileset.

![delete-fileset](./assets/webui/delete-fileset.png)

### Topic

Click the kafka schema tree node on the left sidebar or the schema name link in the table cell.

Displays the list topics of the schema.

![list-topics](./assets/webui/list-topics.png)

#### Create topic

Click on the `CREATE TOPIC` button displays the dialog to create a topic.

![create-topic](./assets/webui/create-topic.png)

Creating a topic needs these fields:

1. **Name**(**_required_**): the name of the topic.
2. **Comment**(_optional_): the comment of the topic.
3. **Properties**(_optional_): Click on the `ADD PROPERTY` button to add custom properties.

#### Show topic details

Click on the action icon <Icon icon='bx:show-alt' fontSize='24' /> in the table cell.

You can see the detailed information of this topic in the drawer component on the right.

![topic-details](./assets/webui/topic-drawer-details.png)

Click the topic tree node on the left sidebar or the topic name link in the table cell.

You can see the detailed information on the right page.

![topic-details](./assets/webui/topic-details.png)

#### Edit topic

Click on the action icon <Icon icon='mdi:square-edit-outline' fontSize='24' /> in the table cell.

Displays the dialog for modifying fields of the selected topic.

![update-topic-dialog](./assets/webui/update-topic-dialog.png)

#### Drop topic

Click on the action icon <Icon icon='mdi:delete-outline' fontSize='24' color='red' /> in the table cell.

Displays a confirmation dialog, clicking on the `DROP` button drops this topic.

![delete-topic](./assets/webui/delete-topic.png)

### Model

Click the model schema tree node on the left sidebar or the schema name link in the table cell.

Displays the list model of the schema.

![list-models](./assets/webui/list-models.png)

#### Register model

Click on the `REGISTER MODEL` button displays the dialog to register a model.

![register-model](./assets/webui/register-model.png)

Register a model needs these fields:

1. **Name**(**_required_**): the name of the model.
2. **Comment**(_optional_): the comment of the model.
3. **Properties**(_optional_): Click on the `ADD PROPERTY` button to add custom properties.

#### Show model details

Click on the action icon <Icon icon='bx:show-alt' fontSize='24' /> in the table cell.

You can see the detailed information of this model in the drawer component on the right.

![model-details](./assets/webui/model-details.png)

#### Drop model

Click on the action icon <Icon icon='mdi:delete-outline' fontSize='24' color='red' /> in the table cell.

Displays a confirmation dialog, clicking on the `DROP` button drops this model.

![delete-model](./assets/webui/delete-model.png)

### Version

Click the model tree node on the left sidebar or the model name link in the table cell.

Displays the list versions of the model.

![list-model-versions](./assets/webui/list-model-versions.png)

#### Link version

Click on the `LINK VERSION` button displays the dialog to link a version.

![link-version](./assets/webui/link-version.png)

Link a version needs these fields:

1. **URI**(**_required_**): the uri of the version.
2. **Aliases**(**_required_**): the aliases of the version, an alias cannot be a number or number string.
3. **Comment**(_optional_): the comment of the model.
4. **Properties**(_optional_): Click on the `ADD PROPERTY` button to add custom properties.

#### Show version details

Click on the action icon <Icon icon='bx:show-alt' fontSize='24' /> in the table cell.

You can see the detailed information of this version in the drawer component on the right.

![version-details](./assets/webui/version-details.png)

#### Drop version

Click on the action icon <Icon icon='mdi:delete-outline' fontSize='24' color='red' /> in the table cell.

Displays a confirmation dialog, clicking on the `DROP` button drops this version.

![delete-version](./assets/webui/delete-version.png)

## Feature capabilities

| Page     | Capabilities                                                                      |
| -------- | --------------------------------------------------------------------------------- |
| Metalake | _`View`_ &#10004; / _`Create`_ &#10004; / _`Edit`_ &#10004; / _`Delete`_ &#10004; |
| Catalog  | _`View`_ &#10004; / _`Create`_ &#10004; / _`Edit`_ &#10004; / _`Delete`_ &#10004; |
| Schema   | _`View`_ &#10004; / _`Create`_ &#10004; / _`Edit`_ &#10004; / _`Delete`_ &#10004; |
| Table    | _`View`_ &#10004; / _`Create`_ &#10004; / _`Edit`_ &#10004; / _`Delete`_ &#10004; |
| Fileset  | _`View`_ &#10004; / _`Create`_ &#10004; / _`Edit`_ &#10004; / _`Delete`_ &#10004; |
| Topic    | _`View`_ &#10004; / _`Create`_ &#10004; / _`Edit`_ &#10004; / _`Delete`_ &#10004; |
| Model    | _`View`_ &#10004; / _`Create`_ &#10004; / _`Edit`_ &#10008; / _`Delete`_ &#10004; |
| Version  | _`View`_ &#10004; / _`Create`_ &#10004; / _`Edit`_ &#10008; / _`Delete`_ &#10004; |

## E2E test

End-to-end testing for web frontends is conducted using the [Selenium](https://www.selenium.dev/documentation/) testing framework, which is Java-based.

Test cases can be found in the project directory: `integration-test/src/test/java/org/apache/gravitino/integration/test/web/ui`, where the `pages` directory is designated for storing definitions of frontend elements, among others.
The root directory contains the actual steps for the test cases.

:::tip
While writing test cases, running them in a local environment may not pose any issues.

However, due to the limited performance capabilities of GitHub Actions, scenarios involving delayed DOM loading—such as the time taken for a popup animation to open—can result in test failures.

To circumvent this issue, it is necessary to manually insert a delay operation, for instance, by adding such as `Thread.sleep(sleepTimeMillis)`.

This ensures that the test waits for the completion of the delay animation before proceeding with the next operation, thereby avoiding the problem.

It is advisable to utilize the [`waits`](https://www.selenium.dev/documentation/webdriver/waits/) methods inherent to Selenium as a substitute for `Thread.sleep()`.
:::
