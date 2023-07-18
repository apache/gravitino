/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton.proto;

class AuditInfoSerDe implements ProtoSerDe<com.datastrato.graviton.meta.AuditInfo, AuditInfo> {
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
