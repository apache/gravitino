/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.proto;

import java.util.Optional;

/** A class for serializing and deserializing AuditInfo objects. */
class AuditInfoSerDe implements ProtoSerDe<com.datastrato.gravitino.meta.AuditInfo, AuditInfo> {

  /**
   * Serializes an {@link com.datastrato.gravitino.meta.AuditInfo} object to a {@link AuditInfo}
   * object.
   *
   * @param auditInfo The AuditInfo object to be serialized.
   * @return The serialized AuditInfo object.
   */
  @Override
  public AuditInfo serialize(com.datastrato.gravitino.meta.AuditInfo auditInfo) {
    AuditInfo.Builder builder = AuditInfo.newBuilder();

    Optional.ofNullable(auditInfo.creator()).ifPresent(builder::setCreator);
    Optional.ofNullable(auditInfo.createTime())
        .map(ProtoUtils::fromInstant)
        .ifPresent(builder::setCreateTime);
    Optional.ofNullable(auditInfo.lastModifier()).ifPresent(builder::setLastModifier);
    Optional.ofNullable(auditInfo.lastModifiedTime())
        .map(ProtoUtils::fromInstant)
        .ifPresent(builder::setLastModifiedTime);

    return builder.build();
  }

  /**
   * Deserializes a {@link AuditInfo} object to an {@link com.datastrato.gravitino.meta.AuditInfo}
   * object.
   *
   * @param p The serialized AuditInfo object.
   * @return The deserialized AuditInfo object.
   */
  @Override
  public com.datastrato.gravitino.meta.AuditInfo deserialize(AuditInfo p) {
    com.datastrato.gravitino.meta.AuditInfo.Builder builder =
        com.datastrato.gravitino.meta.AuditInfo.builder();

    if (p.hasCreator()) builder.withCreator(p.getCreator());
    if (p.hasCreateTime()) builder.withCreateTime(ProtoUtils.toInstant(p.getCreateTime()));
    if (p.hasLastModifier()) builder.withLastModifier(p.getLastModifier());
    if (p.hasLastModifiedTime()) {
      builder.withLastModifiedTime(ProtoUtils.toInstant(p.getLastModifiedTime()));
    }

    return builder.build();
  }
}
