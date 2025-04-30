---
title: Apache Gravitino Trino connector configuration
slug: /trino-connector/configuration
keyword: gravitino connector trino
license: "This software is licensed under the Apache License version 2."
---

<table>
<thead>
<tr>
  <th>Property</th>
  <th>Type</th>
  <th>Default value</th>
  <th>Description</th>
  <th>Required</th>
  <th>Since Version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>connector.name</tt></td>
  <td><tt>string</tt></td>
  <td>(none)</td>
  <td>
    The `connector.name` defines the type of Trino connector.
    This value is always 'gravitino'.
  </td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.metalake</tt></td>
  <td><tt>string</tt></td>
  <td>(none)</td>
  <td>
    The `gravitino.metalake` defines which metalake in Gravitino server the Trino connector uses.
    The Trino connector should set it at startup.
    The value of `gravitino.metalake` needs to be a valid name.
    The Trino connector can detect and load the metalake with catalogs,
    schemas and tables once created and keep in sync.
  </td>
  <td>Yes</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>gravitino.uri</tt></td>
  <td>string</td>
  <td>`http://localhost:8090`</td>
  <td>
    The `gravitino.uri` defines the connection URL of the Gravitino server.
    The default value is `http://localhost:8090`.
    The Trino connector can detect and connect to Gravitino server once it is ready.
    There is  no need to start Gravitino server beforehand.
  </td>
  <td>No</td>
  <td>`0.2.0`</td>
</tr>
<tr>
  <td><tt>trino.jdbc.user</tt></td>
  <td><tt>string</tt></td>
  <td>`admin`</td>
  <td>The JDBC user name for Trino.</td>
  <td>No</td>
  <td>`0.5.1`</td>
</tr>
<tr>
  <td><tt>trino.jdbc.password</tt></td>
  <td><tt>string</tt></td>
  <td>(none)</td>
  <td>The JDBC password for Trino.</td>
  <td>No</td>
  <td>`0.5.1`</td>
</tr>
</tbody>
</table>

