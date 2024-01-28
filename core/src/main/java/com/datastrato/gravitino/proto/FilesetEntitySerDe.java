/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.FilesetEntity;

public class FilesetEntitySerDe implements ProtoSerDe<FilesetEntity, Fileset> {
  @Override
  public Fileset serialize(FilesetEntity filesetEntity) {
    Fileset.Builder builder =
        Fileset.newBuilder()
            .setId(filesetEntity.id())
            .setName(filesetEntity.name())
            .setStorageLocation(filesetEntity.storageLocation())
            .setAuditInfo(new AuditInfoSerDe().serialize(filesetEntity.auditInfo()));

    if (filesetEntity.comment() != null) {
      builder.setComment(filesetEntity.comment());
    }

    if (filesetEntity.properties() != null && !filesetEntity.properties().isEmpty()) {
      builder.putAllProperties(filesetEntity.properties());
    }

    Fileset.Type type = Fileset.Type.valueOf(filesetEntity.filesetType().name());
    builder.setType(type);

    return builder.build();
  }

  @Override
  public FilesetEntity deserialize(Fileset p) {
    FilesetEntity.Builder builder =
        new FilesetEntity.Builder()
            .withId(p.getId())
            .withName(p.getName())
            .withStorageLocation(p.getStorageLocation())
            .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()))
            .withFilesetType(
                com.datastrato.gravitino.file.Fileset.Type.valueOf(p.getType().name()));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    if (p.getPropertiesCount() > 0) {
      builder.withProperties(p.getPropertiesMap());
    }

    return builder.build();
  }
}
