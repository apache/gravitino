package com.datastrato.graviton.proto;

class LakehouseSerDe implements ProtoSerDe<com.datastrato.graviton.meta.Lakehouse, Lakehouse> {
  @Override
  public Lakehouse serialize(com.datastrato.graviton.meta.Lakehouse lakehouse) {
    Lakehouse.Builder builder =
        Lakehouse.newBuilder()
            .setId(lakehouse.getId())
            .setName(lakehouse.getName())
            .setAuditInfo(new AuditInfoSerDe().serialize(lakehouse.auditInfo()));

    if (lakehouse.getComment() != null) {
      builder.setComment(lakehouse.getComment());
    }

    if (lakehouse.getProperties() != null && !lakehouse.getProperties().isEmpty()) {
      builder.putAllProperties(lakehouse.getProperties());
    }

    SchemaVersion version =
        SchemaVersion.newBuilder()
            .setMajorNumber(lakehouse.getVersion().majorVersion)
            .setMinorNumber(lakehouse.getVersion().minorVersion)
            .build();
    builder.setVersion(version);

    return builder.build();
  }

  @Override
  public com.datastrato.graviton.meta.Lakehouse deserialize(Lakehouse p) {
    com.datastrato.graviton.meta.Lakehouse.Builder builder =
        new com.datastrato.graviton.meta.Lakehouse.Builder();
    builder
        .withId(p.getId())
        .withName(p.getName())
        .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    if (p.getPropertiesCount() > 0) {
      builder.withProperties(p.getPropertiesMap());
    }

    com.datastrato.graviton.meta.SchemaVersion version =
        com.datastrato.graviton.meta.SchemaVersion.forValues(
            p.getVersion().getMajorNumber(), p.getVersion().getMinorNumber());
    builder.withVersion(version);

    return builder.build();
  }
}
