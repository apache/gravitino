---
title: 'Apache Gravitino web UI'
slug: /webui
keyword: webui
last_update:
  date: 2024-10-30
  author: LauraXia123
license: 'This software is licensed under the Apache License version 2.'
---

This document shows how to manage metadata using Apache Gravitino Web UI.
The Web UI is the graphical interface accessible through a web browser
as an alternative to writing code or using the REST interface.

Currently, you can integrate [OAuth authentication](../security/index.md)
to view, add, modify, and delete metalakes, create catalogs,
and view catalogs, schemas, and tables, among other functions.

[Build](../develop/how-to-build.md) and [deploy](../getting-started/index.md#local-workstation)
the Gravitino Web UI and open it in a browser at `http://<gravitino-host>:<gravitino-port>`.
by default is [http://localhost:8090](http://localhost:8090).

## Home page

The Web UI homepage displayed in Gravitino depends on the configuration parameter for OAuth mode,
see the details in [Security](../security/index.md).

Set parameter for `gravitino.authenticators`, [`simple`](#simple-mode) or [`oauth`](#oauth-mode).
Simple mode is the default authentication option.
If multiple authenticators are set, the first one is taken by default.

:::tip
After changing the configuration, make sure to restart the Gravitino server.

`${GRAVITINO_HOME}/bin/gravitino.sh restart`
:::

### Simple mode

```text
gravitino.authenticators = simple
```

Set the configuration parameter `gravitino.authenticators` to `simple`,
and the web UI displays the homepage (Metalakes).

![webui-metalakes-simple](../assets/webui/metalakes-simple.png)

At the top-right, the UI shows the current Gravitino version.

The main content displays the existing metalake list.

### Oauth mode

```text
gravitino.authenticators = oauth
```

Set the configuration parameter `gravitino.authenticators` to `oauth`,
and the web UI displays the login page.

:::caution
If both `OAuth` and `HTTPS` are set, due to the different security permission rules of various browsers,
it is recommended to use the [Chrome](https://www.google.com/chrome/) browser for access and operation
to avoid cross-domain errors,

Such as Safari need to enable the developer menu, and select `Disable Cross-Origin Restrictions` from the develop menu.
:::

![webui-login-with-oauth](../assets/webui/login-with-oauth.png)

1. Enter the values corresponding to your specific configuration.
   For detailed instructions, please refer to [Security](../security/index.md).

1. Click on the `LOGIN` button takes you to the homepage.

   ![webui-metalakes-oauth](../assets/webui/metalakes-oauth.png)

   At the top-right, there is an icon button that takes you to the login page when clicked.

## Manage metadata

All the manage actions are performed by calling the [REST API](../api/rest/gravitino-rest-api)

## Features

<table>
<thead>
<tr>
  <th>Object</th>
  <th>Create</th>
  <th>View</th>
  <th>Update</th>
  <th>Delete</th>
  <th>Details</th>
</tr>
</thead>
<tbody>
<tr>
  <td>Metalake</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>[link](./webui-reference/metalakes.md)</td>
</tr>
<tr>
  <td>Catalog</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>[link](./webui-reference/catalogs.md)</td>
</tr>
<tr>
  <td>Schema</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>[link](./webui-reference/catalogs.md)</td>
</tr>
<tr>
  <td>Table</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>[link](./webui-reference/tables.md)</td>
</tr>
<tr>
  <td>Filesets</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>[link](./webui-reference/filesets.md)</td>
</tr>
<tr>
  <td>Topics</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>[link](./webui-reference/topics.md)</td>
</tr>
<tr>
  <td>Models</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10008;</td>
  <td>&#10004;</td>
  <td>[link](./webui-reference/models.md#model)</td>
</tr>
<tr>
  <td>ModelVersions</td>
  <td>&#10004;</td>
  <td>&#10004;</td>
  <td>&#10008;</td>
  <td>&#10004;</td>
  <td>[link](./webui-reference/models.md#modelversion)</td>
</tr>
</tbody>
</table>

