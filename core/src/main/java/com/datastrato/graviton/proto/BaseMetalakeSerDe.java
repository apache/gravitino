/*
* Copyright 2023 Datastrato.
* This software is licensed under the Apache License version 2.
*/

package com.datastrato.graviton.proto;

import com.datastrato.graviton.meta.AuditInfo;

class BaseMetalakeSerDe implements ProtoSerDe<com.datastrato.graviton.meta.BaseMetalake, Metalake> {
  @Override
  public Metalake serialize(com.datastrato.graviton.meta.BaseMetalake baseMetalake) {
    Metalake.Builder builder =
        Metalake.newBuilder()
            .setId(baseMetalake.getId())
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

  @Override
  public com.datastrato.graviton.meta.BaseMetalake deserialize(Metalake p) {
    com.datastrato.graviton.meta.BaseMetalake.Builder builder =
        new com.datastrato.graviton.meta.BaseMetalake.Builder();
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
