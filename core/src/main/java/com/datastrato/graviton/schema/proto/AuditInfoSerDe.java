package com.datastrato.graviton.schema.proto;

class AuditInfoSerDe implements ProtoSerDe<com.datastrato.graviton.schema.AuditInfo, AuditInfo> {
  @Override
  public AuditInfo serialize(com.datastrato.graviton.schema.AuditInfo auditInfo) {
    AuditInfo.Builder builder =
        AuditInfo.newBuilder()
            .setCreator(auditInfo.getCreator())
            .setCreateTime(ProtoUtils.fromInstant(auditInfo.getCreateTime()));

    if (auditInfo.getLastModifier() != null && auditInfo.getLastModifiedTime() != null) {
      builder
          .setLastModifier(auditInfo.getLastModifier())
          .setLastModifiedTime(ProtoUtils.fromInstant(auditInfo.getLastModifiedTime()));
    }

    if (auditInfo.getLastAccessUser() != null && auditInfo.getLastAccessTime() != null) {
      builder
          .setLastAccessUser(auditInfo.getLastAccessUser())
          .setLastAccessTime(ProtoUtils.fromInstant(auditInfo.getLastAccessTime()));
    }

    return builder.build();
  }

  @Override
  public com.datastrato.graviton.schema.AuditInfo deserialize(AuditInfo p) {
    com.datastrato.graviton.schema.AuditInfo.Builder builder =
        new com.datastrato.graviton.schema.AuditInfo.Builder()
            .withCreator(p.getCreator())
            .withCreateTime(ProtoUtils.toInstant(p.getCreateTime()));

    if (p.hasLastModifier() && p.hasLastModifiedTime()) {
      builder
          .withLastModifier(p.getLastModifier())
          .withLastModifiedTime(ProtoUtils.toInstant(p.getLastModifiedTime()));
    }

    if (p.hasLastAccessUser() && p.hasLastAccessTime()) {
      builder
          .withLastAccessUser(p.getLastAccessUser())
          .withLastAccessTime(ProtoUtils.toInstant(p.getLastAccessTime()));
    }

    return builder.build();
  }
}
