# Credential Vending Roadmap

## Background

PR #11149 established the credential vending infrastructure: sensitive credentials are marked
`hidden=true` in PropertiesMetadata, exposed via `propertiesWithCredentialProviders()` using raw
entity properties for `CatalogCredentialManager`, and no longer returned in plaintext by
`properties()` API.

## Completed

| Catalog | Changes |
|---|---|
| JDBC (MySQL/PostgreSQL/Doris/StarRocks) | `jdbc-user`/`jdbc-password` marked hidden; auto-registers `JdbcCredentialProvider` |
| Iceberg | `s3-secret-access-key`, `oss-access-key-secret`, `azure-storage-account-key` marked hidden; `jdbc-user`/`jdbc-password` added and marked hidden; GCS property added |
| Hive | S3/OSS/Azure/GCS storage credential properties added; secret keys marked hidden; credential vending support added |
| BaseCatalog | GCS added to `addStorageCredentialProviders()`; `shouldBackfillCredential()` and `backfillIfPresent()` extracted to base class |

## Pending: Sensitive Credentials Still Passed via `bypass`

These cases still store credentials in plaintext catalog properties, readable via `properties()` API.
All need to be migrated to credential vending.

### 1. Fileset catalog — S3
- **Files**: `clients/filesystem-hadoop3/.../GravitinoVirtualFileSystemS3IT.java`
- **Keys**: `gravitino.bypass.fs.s3a.access.key`, `gravitino.bypass.fs.s3a.secret.key`
- **Note**: GVFS accessing S3 directly; credentials hardcoded in catalog properties

### 2. Fileset catalog — S3 credential vending (partially done)
- **Files**: `clients/filesystem-hadoop3/.../GravitinoVirtualFileSystemS3CredentialIT.java`
- **Note**: Credential vending path exists but old bypass path still coexists; needs cleanup

### 3. Hive catalog — S3 (Hadoop side)
- **Files**: `catalogs/catalog-hive/.../CatalogHiveS3IT.java`
- **Keys**: `gravitino.bypass.fs.s3a.access.key`, `gravitino.bypass.fs.s3a.secret.key`
- **Note**: These configure Hive/Hadoop's own access to S3 (HMS side), which is a **different
  concern** from credential vending (vending credentials to compute engines like Spark/Trino).
  Needs careful separation.

### 4. Trino connector — Hive/Iceberg S3 (docs)
- **Files**: `docs/trino-connector/catalog-hive.md`, `docs/trino-connector/catalog-iceberg.md`
- **Keys**: `trino.bypass.hive.s3.aws-access-key`, `trino.bypass.hive.s3.aws-secret-key`
- **Note**: Old S3 config pattern documented for Trino; docs need updating to credential vending

### 5. Iceberg catalog — OSS / custom FileIO token
- **Files**: `docs/lakehouse-iceberg-catalog.md`
- **Keys**: `gravitino.bypass.client.security-token`, `gravitino.bypass.security-token`
- **Note**: OSS STS token or custom FileIO security token; no credential vending support yet

## Migration Approach

1. `bypass` properties are not registered in `PropertiesMetadata` → not filtered by `hidden=true`
   → still returned as plaintext via `properties()` API
2. Fix: register these keys in the catalog's `PropertiesMetadata` with `hidden=true`, and support
   detection in `propertiesWithCredentialProviders()`
3. The Hive `gravitino.bypass.fs.s3a.*` case (HMS-side storage access) is a **different purpose**
   from credential vending (vending to compute engines) — handle separately

## Priority

1. Fileset catalog (high user impact)
2. Trino connector docs update
3. Iceberg OSS token
