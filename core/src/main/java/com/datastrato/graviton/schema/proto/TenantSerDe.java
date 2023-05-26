package com.datastrato.graviton.schema.proto;

class TenantSerDe implements ProtoSerDe<com.datastrato.graviton.schema.Tenant, Tenant> {
  @Override
  public Tenant serialize(com.datastrato.graviton.schema.Tenant tenant) {
    Tenant.Builder builder =
        Tenant.newBuilder()
            .setId(tenant.getId())
            .setName(tenant.getName())
            .setAuditInfo(new AuditInfoSerDe().serialize(tenant.auditInfo()));

    if (tenant.getComment() != null) {
      builder.setComment(tenant.getComment());
    }

    SchemaVersion version =
        SchemaVersion.newBuilder()
            .setMajorNumber(tenant.getVersion().majorVersion)
            .setMinorNumber(tenant.getVersion().minorVersion)
            .build();

    builder.setVersion(version);

    return builder.build();
  }

  @Override
  public com.datastrato.graviton.schema.Tenant deserialize(Tenant p) {
    com.datastrato.graviton.schema.Tenant.Builder builder =
        new com.datastrato.graviton.schema.Tenant.Builder();
    builder
        .withId(p.getId())
        .withName(p.getName())
        .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    com.datastrato.graviton.schema.SchemaVersion version =
        com.datastrato.graviton.schema.SchemaVersion.forValues(
            p.getVersion().getMajorNumber(), p.getVersion().getMinorNumber());
    builder.withVersion(version);

    return builder.build();
  }
}
