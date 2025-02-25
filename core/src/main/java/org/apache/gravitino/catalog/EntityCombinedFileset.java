/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Audit;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FilesetEntity;

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

  public EntityCombinedFileset withHiddenProperties(Set<String> hiddenProperties) {
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
        .filter(entry -> entry.getKey() != null && entry.getValue() != null)
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
