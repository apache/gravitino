### Catalog Operations

Refer to [Manage Relational Metadata Using Gravitino](./manage-relational-metadata-using-gravitino.md#catalog-operations) for more details.

:::note
Sensitive catalog properties such as `jdbc-user` and `jdbc-password` are hidden from the load catalog response since Gravitino 1.3.0. Use the [credential vending API](security/credential-vending.md) to retrieve them at runtime.
:::

## Schema