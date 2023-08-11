/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.proto;

/** A class for serializing and deserializing AuditInfo objects. */
class AuditInfoSerDe implements ProtoSerDe<com.datastrato.graviton.meta.AuditInfo, AuditInfo> {

  /**
   * Serializes an {@link com.datastrato.graviton.meta.AuditInfo} object to a {@link AuditInfo}
   * object.
   *
   * @param auditInfo The AuditInfo object to be serialized.
   * @return The serialized AuditInfo object.
   */
  @Override
  public AuditInfo serialize(com.datastrato.graviton.meta.AuditInfo auditInfo) {
    AuditInfo.Builder builder =
        AuditInfo.newBuilder()
            .setCreator(auditInfo.creator())
            .setCreateTime(ProtoUtils.fromInstant(auditInfo.createTime()));

    if (auditInfo.lastModifier() != null && auditInfo.lastModifiedTime() != null) {
      builder
          .setLastModifier(auditInfo.lastModifier())
          .setLastModifiedTime(ProtoUtils.fromInstant(auditInfo.lastModifiedTime()));
    }

    return builder.build();
  }

  /**
   * Deserializes a {@link AuditInfo} object to an {@link com.datastrato.graviton.meta.AuditInfo}
   * object.
   *
   * @param p The serialized AuditInfo object.
   * @return The deserialized AuditInfo object.
   */
  @Override
  public com.datastrato.graviton.meta.AuditInfo deserialize(AuditInfo p) {
    com.datastrato.graviton.meta.AuditInfo.Builder builder =
        new com.datastrato.graviton.meta.AuditInfo.Builder()
            .withCreator(p.getCreator())
            .withCreateTime(ProtoUtils.toInstant(p.getCreateTime()));

    if (p.hasLastModifier() && p.hasLastModifiedTime()) {
      builder
          .withLastModifier(p.getLastModifier())
          .withLastModifiedTime(ProtoUtils.toInstant(p.getLastModifiedTime()));
    }

    return builder.build();
  }
}
