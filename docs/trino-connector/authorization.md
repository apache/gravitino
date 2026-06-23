---
title: "Apache Gravitino Trino connector Authorization"
slug: /trino-connector/authorization
keyword: gravitino connector trino authorization rbac access control
license: "This software is licensed under the Apache License version 2."
---

## Authorization

The Gravitino Trino connector can authenticate to the Gravitino server, but query
authorization also depends on how Trino authenticates users and how the deployment
binds the Trino session user to Gravitino policies.

For multi-user deployments, distinguish the following identities:

- **Trino end user**: the user authenticated by Trino for a query session.
- **Trino service identity**: the credential used by the Trino connector to call
  the Gravitino server.
- **Gravitino user**: the principal used when Gravitino evaluates RBAC policies.

These identities may be the same in simple test deployments, but production
deployments should define how they are mapped and audited.

### Session User Forwarding

When `gravitino.client.session.forwardUser=true` is used with
`gravitino.client.authType=simple`, the connector creates per-session Gravitino
clients and forwards the Trino session user to Gravitino.

This mode is useful when Trino has already authenticated the session user and the
deployment is comfortable using the Trino session user as the Gravitino simple
authentication user.

Do not enable this mode for anonymous or shared-user Trino deployments. If a BI,
JDBC, or ODBC client connects to Trino with a shared account, Gravitino can only
see that shared account unless the deployment has another trusted identity
mapping mechanism.

### OAuth and BI Clients

Some BI, JDBC, or ODBC clients may not be able to forward an OAuth token whose
audience is the Gravitino server. In those deployments, requiring the end-user
OAuth token to be propagated from the client to Trino and then to Gravitino may
not be practical.

If Trino is the system that authenticates users, the deployment should treat the
authenticated Trino `Identity` as the trusted user boundary. Do not trust a user
name supplied directly by the client unless Trino has authenticated it and
applied user mapping or impersonation checks.

### Access Control in Trino

Trino provides `SystemAccessControl` hooks for query authorization, such as table
selection, insertion, update, deletion, and DDL checks. A deployment can use a
Trino access-control plugin to call an external policy decision point before
allowing a query operation.

When Gravitino RBAC is used as the policy source, a Trino access-control
integration should map Trino operations to Gravitino metadata objects and
privileges. A typical mapping is:

| Trino operation | Gravitino privilege |
|-----------------|---------------------|
| Access catalog | `USE_CATALOG` |
| Access schema | `USE_SCHEMA` |
| Create schema | `CREATE_SCHEMA` |
| Create table | `CREATE_TABLE` |
| Read table columns or table metadata | `SELECT_TABLE` |
| Insert, update, delete, or alter table data | `MODIFY_TABLE` |
| Create view | `CREATE_VIEW` |
| Read view | `SELECT_VIEW` |

Such an integration should fail closed when the policy decision cannot be
validated.

### Security Considerations

For user-level authorization with Trino and Gravitino:

- Enable real user authentication in Trino.
- Configure Trino user mapping and impersonation rules so clients cannot spoof
  another user.
- Keep Trino service authentication to Gravitino separate from end-user
  authorization decisions.
- Audit both the trusted Trino service identity and the delegated end user when a
  service identity asks Gravitino to evaluate policies for a user.
- Avoid maintaining a second, divergent policy model in Trino when Gravitino RBAC
  is intended to be the central policy source.

### See Also

- [Authentication](authentication.md)
- [Configuration](configuration.md)
