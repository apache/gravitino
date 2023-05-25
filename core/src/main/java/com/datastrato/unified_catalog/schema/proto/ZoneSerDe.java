package com.datastrato.unified_catalog.schema.proto;

class ZoneSerDe implements ProtoSerDe<com.datastrato.unified_catalog.schema.Zone, Zone> {
  @Override
  public Zone serialize(com.datastrato.unified_catalog.schema.Zone zone) {
    Zone.Builder builder =
        Zone.newBuilder()
            .setId(zone.getId())
            .setLakehouseId(zone.getLakehouseId())
            .setName(zone.getName())
            .setAuditInfo(new AuditInfoSerDe().serialize(zone.auditInfo()));

    if (zone.getComment() != null) {
      builder.setComment(zone.getComment());
    }

    if (zone.getProperties() != null && !zone.getProperties().isEmpty()) {
      builder.putAllProperties(zone.getProperties());
    }

    return builder.build();
  }

  @Override
  public com.datastrato.unified_catalog.schema.Zone deserialize(Zone p) {
    com.datastrato.unified_catalog.schema.Zone.Builder builder =
        new com.datastrato.unified_catalog.schema.Zone.Builder();
    builder
        .withId(p.getId())
        .withLakehouseId(p.getLakehouseId())
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
