/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.FilesetEntity;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class EntityCombinedFileset implements Fileset {

  private final Fileset fileset;

  private final FilesetEntity filesetEntity;

  // Sets of properties that should be hidden from the user.
  private Set<String> hiddenProperties;

  private EntityCombinedFileset(Fileset fileset, FilesetEntity filesetEntity) {
    this.fileset = fileset;
    this.filesetEntity = filesetEntity;
  }

  public static EntityCombinedFileset of(Fileset fileset, FilesetEntity filesetEntity) {
    return new EntityCombinedFileset(fileset, filesetEntity);
  }

  public static EntityCombinedFileset of(Fileset fileset) {
    return new EntityCombinedFileset(fileset, null);
  }

  public EntityCombinedFileset withHiddenPropertiesSet(Set<String> hiddenProperties) {
    this.hiddenProperties = hiddenProperties;
    return this;
  }

  @Override
  public String name() {
    return fileset.name();
  }

  @Override
  public String comment() {
    return fileset.comment();
  }

  @Override
  public Type type() {
    return fileset.type();
  }

  @Override
  public String storageLocation() {
    return fileset.storageLocation();
  }

  @Override
  public Map<String, String> properties() {
    return fileset.properties().entrySet().stream()
        .filter(p -> !hiddenProperties.contains(p.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Audit auditInfo() {
    AuditInfo mergedAudit =
        AuditInfo.builder()
            .withCreator(fileset.auditInfo().creator())
            .withCreateTime(fileset.auditInfo().createTime())
            .withLastModifier(fileset.auditInfo().lastModifier())
            .withLastModifiedTime(fileset.auditInfo().lastModifiedTime())
            .build();

    return filesetEntity == null
        ? fileset.auditInfo()
        : mergedAudit.merge(filesetEntity.auditInfo(), true /* overwrite */);
  }
}
