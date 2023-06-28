package com.datastrato.graviton.proto;

import com.datastrato.graviton.meta.AuditInfo;

class BaseLakehouseSerDe
    implements ProtoSerDe<com.datastrato.graviton.meta.BaseLakehouse, Lakehouse> {
  @Override
  public Lakehouse serialize(com.datastrato.graviton.meta.BaseLakehouse baseLakehouse) {
    Lakehouse.Builder builder =
        Lakehouse.newBuilder()
            .setId(baseLakehouse.getId())
            .setName(baseLakehouse.name())
            .setAuditInfo(new AuditInfoSerDe().serialize((AuditInfo) baseLakehouse.auditInfo()));

    if (baseLakehouse.comment() != null) {
      builder.setComment(baseLakehouse.comment());
    }

    if (baseLakehouse.properties() != null && !baseLakehouse.properties().isEmpty()) {
      builder.putAllProperties(baseLakehouse.properties());
    }

    SchemaVersion version =
        SchemaVersion.newBuilder()
            .setMajorNumber(baseLakehouse.getVersion().majorVersion)
            .setMinorNumber(baseLakehouse.getVersion().minorVersion)
            .build();
    builder.setVersion(version);

    return builder.build();
  }

  @Override
  public com.datastrato.graviton.meta.BaseLakehouse deserialize(Lakehouse p) {
    com.datastrato.graviton.meta.BaseLakehouse.Builder builder =
        new com.datastrato.graviton.meta.BaseLakehouse.Builder();
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
