/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.proto;

import com.datastrato.gravitino.meta.FileEntity;

public class FileEntitySerDe implements ProtoSerDe<FileEntity, File> {
  @Override
  public File serialize(FileEntity fileEntity) {
    File.Builder builder =
        File.newBuilder()
            .setId(fileEntity.id())
            .setName(fileEntity.name())
            .setAuditInfo(new AuditInfoSerDe().serialize(fileEntity.auditInfo()));

    if (fileEntity.comment() != null) {
      builder.setComment(fileEntity.comment());
    }

    if (fileEntity.properties() != null && !fileEntity.properties().isEmpty()) {
      builder.putAllProperties(fileEntity.properties());
    }

    File.Type type = File.Type.valueOf(fileEntity.fileType().name());
    builder.setType(type);

    if (fileEntity.storageLocation() != null) {
      builder.setStorageLocation(fileEntity.storageLocation());
    }

    return builder.build();
  }

  @Override
  public FileEntity deserialize(File p) {
    FileEntity.Builder builder =
        new FileEntity.Builder()
            .withId(p.getId())
            .withName(p.getName())
            .withAuditInfo(new AuditInfoSerDe().deserialize(p.getAuditInfo()))
            .withFileType(com.datastrato.gravitino.file.File.Type.valueOf(p.getType().name()));

    if (p.hasComment()) {
      builder.withComment(p.getComment());
    }

    if (p.hasStorageLocation()) {
      builder.withStorageLocation(p.getStorageLocation());
    }

    if (p.getPropertiesCount() > 0) {
      builder.withProperties(p.getPropertiesMap());
    }

    return builder.build();
  }
}
