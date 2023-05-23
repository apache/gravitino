package com.datastrato.unified_catalog.schema.proto;

public class TenantSerDe
    implements ProtoSerDe<com.datastrato.unified_catalog.schema.Tenant, Tenant> {
  @Override
  public Tenant serialize(com.datastrato.unified_catalog.schema.Tenant tenant) {
    Tenant.Builder builder = Tenant.newBuilder()
        .setId(tenant.getId())
        .setName(tenant.getName())
        .setAuditInfo(new AuditInfoSerDe().serialize(tenant.auditInfo()));

    if (tenant.getComment() != null) {
      builder.setComment(tenant.getComment());
    }

    SchemaVersion version = SchemaVersion.newBuilder()
        .setMajorNumber(tenant.getVersion().majorVersion)
        .setMinorNumber(tenant.getVersion().minorVersion)
        .build();

    builder.setVersion(version);

    return builder.build();
  }

  @Override
  public com.datastrato.unified_catalog.schema.Tenant deserialize(Tenant p) {
    com.datastrato.unified_catalog.schema.Tenant.Builder builder =
        new com.datastrato.unified_catalog.schema.Tenant.Builder();
    builder.withId(p.getId())
        .withName(p.getName())
        .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    com.datastrato.unified_catalog.schema.SchemaVersion version =
        com.datastrato.unified_catalog.schema.SchemaVersion.forValues(
            p.getVersion().getMajorNumber(), p.getVersion().getMinorNumber());
    builder.withVersion(version);

    return builder.build();
  }
}
