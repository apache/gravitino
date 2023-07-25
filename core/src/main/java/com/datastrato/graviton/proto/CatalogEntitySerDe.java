/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.proto;

import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.CatalogEntity;
import com.google.common.collect.ImmutableList;

public class CatalogEntitySerDe implements ProtoSerDe<CatalogEntity, Catalog> {
  @Override
  public Catalog serialize(CatalogEntity catalogEntity) {
    Catalog.Builder builder =
        Catalog.newBuilder()
            .setId(catalogEntity.getId())
            .setName(catalogEntity.name())
            .setAuditInfo(new AuditInfoSerDe().serialize((AuditInfo) catalogEntity.auditInfo()));

    if (catalogEntity.getComment() != null) {
      builder.setComment(catalogEntity.getComment());
    }

    if (catalogEntity.getProperties() != null && !catalogEntity.getProperties().isEmpty()) {
      builder.putAllProperties(catalogEntity.getProperties());
    }

    com.datastrato.graviton.proto.Catalog.Type type =
        com.datastrato.graviton.proto.Catalog.Type.valueOf(catalogEntity.getType().name());
    builder.setType(type);

    builder.setMetalakeId(catalogEntity.getMetalakeId());

    com.datastrato.graviton.proto.Namespace namespace =
        Namespace.newBuilder()
            .addAllLevels(ImmutableList.copyOf(catalogEntity.namespace().levels()))
            .build();
    builder.setNamespace(namespace);
    return builder.build();
  }

  @Override
  public CatalogEntity deserialize(Catalog p) {
    CatalogEntity.Builder builder = new CatalogEntity.Builder();
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

    builder.withType(com.datastrato.graviton.Catalog.Type.valueOf(p.getType().name()));
    builder.withMetalakeId(p.getMetalakeId());
    builder.withNamespace(
        com.datastrato.graviton.Namespace.of(
            p.getNamespace().getLevelsList().toArray(new String[0])));
    return builder.build();
  }
}
