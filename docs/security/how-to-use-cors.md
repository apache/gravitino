---
title: "CORS"
slug: "/security/how-to-use-cors"
keywords:
  - security
  - cors
license: "This software is licensed under the Apache License version 2."
---

## Overview

Cross-Origin Resource Sharing is a browser mechanism that controls which web origins may call an HTTP API. A browser blocks a request from a page whose origin differs from the server's in host, port, or protocol unless the server says otherwise, so any browser-based client hosted separately from Gravitino needs the CORS filter enabled.

The filter is off by default and applies only to browsers. Requests from engines, the CLI, and other server-side clients are unaffected either way.

## Configuration

The Gravitino server and the Iceberg REST service each have their own CORS filter, configured with the same property names under different prefixes. Use `gravitino.server.webserver.` for the Gravitino server and `gravitino.iceberg-rest.` for the Iceberg REST service, and set each one only for the service that needs it.

| Property Name           | Description                                                                                 | Default Value                                 |
|-------------------------|---------------------------------------------------------------------------------------------|-----------------------------------------------|
| `enableCorsFilter`      | Enables the CORS filter                                                                     | `false`                                       |
| `allowedOrigins`        | Comma-separated origins allowed to access the resources, or `*` for all                     | `*`                                           |
| `allowedTimingOrigins`  | Comma-separated origins allowed to time the resources. Empty means none                     | (empty)                                       |
| `allowedMethods`        | Comma-separated HTTP methods allowed when accessing the resources                           | `GET,POST,HEAD,DELETE,PUT`                    |
| `allowedHeaders`        | Comma-separated request headers allowed, or a single `*` to accept any header               | `X-Requested-With,Content-Type,Accept,Origin` |
| `exposedHeaders`        | Comma-separated response headers made readable to the client. Empty means none              | (empty)                                       |
| `preflightMaxAgeInSecs` | How long a client may cache a preflight response                                            | `1800`                                        |
| `allowCredentials`      | Whether requests carrying credentials are allowed                                           | `true`                                        |
| `chainPreflight`        | Passes preflight requests to the target resource as an `OPTIONS` request instead of answering them in the filter | `true`                    |

### Origins and Credentials

The two defaults do not work together. Browsers reject a response that allows credentials while allowing every origin, so leaving `allowedOrigins` at `*` with `allowCredentials` at `true` means any authenticated browser request fails even though the filter is enabled.

List the origins your clients actually use instead. A wildcard origin is only usable when `allowCredentials` is `false`, which rules out any request carrying a token or a cookie.

### Example

A web UI served from `https://console.example.com` calling a Gravitino server elsewhere needs the following in `gravitino.conf`.

```properties
gravitino.server.webserver.enableCorsFilter = true
gravitino.server.webserver.allowedOrigins = https://console.example.com
gravitino.server.webserver.allowedHeaders = X-Requested-With,Content-Type,Accept,Origin,Authorization
```

`Authorization` is added because the default header list omits it, and a browser sending a bearer token names that header in its preflight request.
