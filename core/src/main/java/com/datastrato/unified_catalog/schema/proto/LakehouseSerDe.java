package com.datastrato.unified_catalog.schema.proto;

class LakehouseSerDe
    implements ProtoSerDe<com.datastrato.unified_catalog.schema.Lakehouse, Lakehouse> {
  @Override
  public Lakehouse serialize(com.datastrato.unified_catalog.schema.Lakehouse lakehouse) {
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

    return builder.build();
  }

  @Override
  public com.datastrato.unified_catalog.schema.Lakehouse deserialize(Lakehouse p) {
    com.datastrato.unified_catalog.schema.Lakehouse.Builder builder =
        new com.datastrato.unified_catalog.schema.Lakehouse.Builder();
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

    return builder.build();
  }
}
