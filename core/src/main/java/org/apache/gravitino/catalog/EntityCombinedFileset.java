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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.Audit;
import org.apache.gravitino.connector.HiddenPropertyMaskUtils;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FilesetEntity;

public final class EntityCombinedFileset implements Fileset {

  private final Fileset fileset;

  private final FilesetEntity filesetEntity;

  // Editable hidden/secret keys are masked; reserved+hidden keys are omitted.
  private Set<String> keysToMask = Collections.emptySet();

  private Set<String> keysToOmit = Collections.emptySet();

  private EntityCombinedFileset(Fileset fileset, FilesetEntity filesetEntity) {
    this.fileset = fileset;
    this.filesetEntity = filesetEntity;
  }

  public FilesetEntity filesetEntity() {
    return filesetEntity;
  }

  public Fileset fileset() {
    return fileset;
  }

  public static EntityCombinedFileset of(Fileset fileset, FilesetEntity filesetEntity) {
    return new EntityCombinedFileset(fileset, filesetEntity);
  }

  public static EntityCombinedFileset of(Fileset fileset) {
    return new EntityCombinedFileset(fileset, null);
  }

  public EntityCombinedFileset withHiddenProperties(Set<String> keysToMask) {
    this.keysToMask = keysToMask == null ? Collections.emptySet() : keysToMask;
    this.keysToOmit = Collections.emptySet();
    return this;
  }

  public EntityCombinedFileset withHiddenProperties(
      Map.Entry<Set<String>, Set<String>> classified) {
    if (classified == null) {
      this.keysToMask = Collections.emptySet();
      this.keysToOmit = Collections.emptySet();
    } else {
      this.keysToMask = classified.getKey() == null ? Collections.emptySet() : classified.getKey();
      this.keysToOmit =
          classified.getValue() == null ? Collections.emptySet() : classified.getValue();
    }
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
  public Map<String, String> storageLocations() {
    return fileset.storageLocations();
  }

  @Override
  public Map<String, String> properties() {
    return HiddenPropertyMaskUtils.maskHiddenProperties(
        fileset.properties(), keysToMask, keysToOmit);
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
