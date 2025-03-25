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
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.model.Model;

public final class EntityCombinedModel implements Model {

  private final Model model;

  private final ModelEntity modelEntity;

  private Set<String> hiddenProperties = Collections.emptySet();

  private EntityCombinedModel(Model model, ModelEntity modelEntity) {
    this.model = model;
    this.modelEntity = modelEntity;
  }

  public static EntityCombinedModel of(Model model, ModelEntity modelEntity) {
    return new EntityCombinedModel(model, modelEntity);
  }

  public static EntityCombinedModel of(Model model) {
    return new EntityCombinedModel(model, null);
  }

  public EntityCombinedModel withHiddenProperties(Set<String> hiddenProperties) {
    this.hiddenProperties = hiddenProperties;
    return this;
  }

  @Override
  public String name() {
    return model.name();
  }

  @Override
  public String comment() {
    return model.comment();
  }

  @Override
  public Map<String, String> properties() {
    return model.properties() == null
        ? null
        : model.properties().entrySet().stream()
            .filter(e -> !hiddenProperties.contains(e.getKey()))
            .filter(entry -> entry.getKey() != null && entry.getValue() != null)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public int latestVersion() {
    return model.latestVersion();
  }

  @Override
  public Audit auditInfo() {
    AuditInfo mergedAudit =
        AuditInfo.builder()
            .withCreator(model.auditInfo().creator())
            .withCreateTime(model.auditInfo().createTime())
            .withLastModifier(model.auditInfo().lastModifier())
            .withLastModifiedTime(model.auditInfo().lastModifiedTime())
            .build();

    return modelEntity == null
        ? mergedAudit
        : mergedAudit.merge(modelEntity.auditInfo(), true /* overwrite */);
  }
}
