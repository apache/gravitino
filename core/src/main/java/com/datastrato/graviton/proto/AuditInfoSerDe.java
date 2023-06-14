package com.datastrato.graviton.proto;

class AuditInfoSerDe implements ProtoSerDe<com.datastrato.graviton.meta.AuditInfo, AuditInfo> {
  @Override
  public AuditInfo serialize(com.datastrato.graviton.meta.AuditInfo auditInfo) {
    AuditInfo.Builder builder =
        AuditInfo.newBuilder()
            .setCreator(auditInfo.getCreator())
            .setCreateTime(ProtoUtils.fromInstant(auditInfo.getCreateTime()));

    if (auditInfo.getLastModifier() != null && auditInfo.getLastModifiedTime() != null) {
      builder
          .setLastModifier(auditInfo.getLastModifier())
          .setLastModifiedTime(ProtoUtils.fromInstant(auditInfo.getLastModifiedTime()));
    }

    return builder.build();
  }

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
