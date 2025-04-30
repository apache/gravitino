---
title: "How to use CORS"
slug: /security/how-to-use-cors
keyword: security cors
license: "This software is licensed under the Apache License version 2."
---

## Cross-origin resource filter

### Server configuration

<table>
<thead>
<tr>
  <th>Configuration Option</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.server.webserver.enableCorsFilter</tt></td>
  <td> Enable cross-origin resource share filter.</td>
  <td> Enable cross-origin resource share filter.</td>
  <td>false</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.allowedOrigins</tt></td>
  <td>
    A comma separated list of allowed origins to access the resources.
    The default value is `*`, which means all origins.
  </td>
  <td>`*`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.allowedTimingOrigins</tt></td>
  <td>
    A comma separated list of allowed origins to time the resource.
    The default value is an empty string, which means no origins.
  </td>
  <td>''(empty string)</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.allowedMethods</tt></td>
  <td>
    A comma separated list of allowed HTTP methods used when accessing the resources.
    The default values are `GET`, `POST`, `HEAD`, and `DELETE`.
  </td>
  <td>`GET,POST,HEAD,DELETE,PUT`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.allowedHeaders</tt></td>
  <td>
    A comma separated list of allowed HTTP headers to be specified when accessing the resources.
    The default value is `X-Requested-With,Content-Type,Accept,Origin`.
    If the value is a single `*`, it means all headers are acceptable.
  </td>
  <td>`X-Requested-With,Content-Type,Accept,Origin`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.preflightMaxAgeInSecs</tt></td>
  <td>
    The number of seconds to cache inflight requests by the client.
    The default value is 1800 seconds or 30 minutes.
  </td>
  <td>`1800`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.allowCredentials</tt></td>
  <td>
    A boolean indicating if the resource allows requests with credentials.
    The default value is `true`.
  </td>
  <td>`true`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.exposedHeaders</tt></td>
  <td>
    A comma separated list of allowed HTTP headers exposed on the client.
    The default value is an empty list.
  </td>
  <td>''(empty string)</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.server.webserver.chainPreflight</tt></td>
  <td>
    If true chained preflight requests for normal handling (as an `OPTION` request).
    Otherwise, the filter responds to the preflight. The default is `true`.
  </td>
  <td>`true`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
</tbody>
</table>


### Apache Iceberg REST service's configuration

<table>
<thead>
<tr>
  <th>Configuration item</th>
  <th>Description</th>
  <th>Default value</th>
  <th>Required</th>
  <th>Since version</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>gravitino.iceberg-rest.enableCorsFilter</tt></td>
  <td> Enable cross-origin resource share filter.</td>
  <td>`false`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.allowedOrigins</tt></td>
  <td>
    A comma separated list of allowed origins that access the resources.
    The default value is `*`, which means all origins are allowed.
   </td>
  <td>`*`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.allowedTimingOrigins</tt></td>
  <td>
    A comma separated list of allowed origins that time the resource.
    The default value is an empty string, which means no origin is allowed.
  </td>
  <td>`''`(empty string)</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.allowedMethods</tt></td>
  <td>
    A comma separated list of allowed HTTP methods used when accessing the resources.
    The default values are `GET`, `POST`, `HEAD`, and `DELETE`.
  </td>
  <td>`GET,POST,HEAD,DELETE,PUT`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.allowedHeaders</tt></td>
  <td>
    A comma separated list of HTTP allowed headers specified when accessing the resources.
    The default value is `X-Requested-With,Content-Type,Accept,Origin`.
    If the value is a single `*`, it means all headers are acceptable.
  </td>
  <td>`X-Requested-With,Content-Type,Accept,Origin`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.preflightMaxAgeInSecs</tt></td>
  <td>
    The number of seconds to cache preflight requests by the client.
    The default value is 1800 seconds (30 minutes).
   </td>
  <td>`1800`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.allowCredentials</tt></td>
  <td>
    A boolean indicating if the resource allows requests with credentials.
    The default value is `true`.
  </td>
  <td>`true`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.exposedHeaders</tt></td>
  <td>
    A comma separated list of allowed HTTP headers exposed on the client.
    The default value is an empty list.
  </td>
  <td>''(empty string)</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
<tr>
  <td><tt>gravitino.iceberg-rest.chainPreflight</tt></td>
  <td>
   If true, chained preflight requests for normal handling (as an `OPTION` request).
   Otherwise, the filter responds to the preflight. The default is `true`.
  </td>
  <td>`true`</td>
  <td>No</td>
  <td>`0.4.0`</td>
</tr>
</tbody>
</table>

