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
import java.util.stream.Collectors;
import org.apache.gravitino.Audit;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.model.ModelVersion;

public final class EntityCombinedModelVersion implements ModelVersion {

  private final ModelVersion modelVersion;

  private final ModelVersionEntity modelVersionEntity;

  private Set<String> hiddenProperties = Collections.emptySet();

  private EntityCombinedModelVersion(
      ModelVersion modelVersion, ModelVersionEntity modelVersionEntity) {
    this.modelVersion = modelVersion;
    this.modelVersionEntity = modelVersionEntity;
  }

  public static EntityCombinedModelVersion of(
      ModelVersion modelVersion, ModelVersionEntity modelVersionEntity) {
    return new EntityCombinedModelVersion(modelVersion, modelVersionEntity);
  }

  public static EntityCombinedModelVersion of(ModelVersion modelVersion) {
    return new EntityCombinedModelVersion(modelVersion, null);
  }

  public EntityCombinedModelVersion withHiddenProperties(Set<String> hiddenProperties) {
    this.hiddenProperties = hiddenProperties;
    return this;
  }

  @Override
  public int version() {
    return modelVersion.version();
  }

  @Override
  public String comment() {
    return modelVersion.comment();
  }

  @Override
  public Map<String, String> properties() {
    return modelVersion.properties() == null
        ? null
        : modelVersion.properties().entrySet().stream()
            .filter(e -> !hiddenProperties.contains(e.getKey()))
            .filter(entry -> entry.getKey() != null && entry.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public String uri() {
    return modelVersion.uri();
  }

  @Override
  public String[] aliases() {
    return modelVersion.aliases();
  }

  @Override
  public Audit auditInfo() {
    AuditInfo mergedAudit =
        AuditInfo.builder()
            .withCreator(modelVersion.auditInfo().creator())
            .withCreateTime(modelVersion.auditInfo().createTime())
            .withLastModifier(modelVersion.auditInfo().lastModifier())
            .withLastModifiedTime(modelVersion.auditInfo().lastModifiedTime())
            .build();

    return modelVersionEntity == null
        ? mergedAudit
        : mergedAudit.merge(modelVersionEntity.auditInfo(), true /* overwrite */);
  }
}
