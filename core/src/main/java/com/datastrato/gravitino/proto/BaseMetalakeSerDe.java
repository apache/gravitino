/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.AuditInfo;

/** A class for serializing and deserializing BaseMetalake objects. */
class BaseMetalakeSerDe
    implements ProtoSerDe<com.datastrato.gravitino.meta.BaseMetalake, Metalake> {

  /**
   * Serializes a {@link com.datastrato.gravitino.meta.BaseMetalake} object to a {@link Metalake}
   * object.
   *
   * @param baseMetalake The BaseMetalake object to be serialized.
   * @return The serialized Metalake object.
   */
  @Override
  public Metalake serialize(com.datastrato.gravitino.meta.BaseMetalake baseMetalake) {
    Metalake.Builder builder =
        Metalake.newBuilder()
            .setId(baseMetalake.id())
            .setName(baseMetalake.name())
            .setAuditInfo(new AuditInfoSerDe().serialize((AuditInfo) baseMetalake.auditInfo()));

    if (baseMetalake.comment() != null) {
      builder.setComment(baseMetalake.comment());
    }

    if (baseMetalake.properties() != null && !baseMetalake.properties().isEmpty()) {
      builder.putAllProperties(baseMetalake.properties());
    }

    SchemaVersion version =
        SchemaVersion.newBuilder()
            .setMajorNumber(baseMetalake.getVersion().majorVersion)
            .setMinorNumber(baseMetalake.getVersion().minorVersion)
            .build();
    builder.setVersion(version);

    return builder.build();
  }

  /**
   * Deserializes a {@link Metalake} object to a {@link com.datastrato.gravitino.meta.BaseMetalake}
   * object.
   *
   * @param p The serialized Metalake object.
   * @return The deserialized BaseMetalake object.
   */
  @Override
  public com.datastrato.gravitino.meta.BaseMetalake deserialize(Metalake p) {
    com.datastrato.gravitino.meta.BaseMetalake.Builder builder =
        com.datastrato.gravitino.meta.BaseMetalake.builder();
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

    com.datastrato.gravitino.meta.SchemaVersion version =
        com.datastrato.gravitino.meta.SchemaVersion.forValues(
            p.getVersion().getMajorNumber(), p.getVersion().getMinorNumber());
    builder.withVersion(version);

    return builder.build();
  }
}
