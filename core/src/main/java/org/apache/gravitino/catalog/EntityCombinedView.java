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
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.View;

/**
 * A View class to represent a view metadata object that combines the metadata both from {@link
 * View} and {@link ViewEntity}.
 */
public final class EntityCombinedView implements View {

  private final View view;

  @Nullable private final ViewEntity viewEntity;

  // Sets of properties that should be hidden from the user.
  private Set<String> hiddenProperties = Collections.emptySet();

  // Field "imported" is used to indicate whether the entity has been imported to Gravitino
  // managed storage backend. If "imported" is true, it means that storage backend have stored
  // the correct entity. Otherwise, we should import the external entity to the storage backend.
  private boolean imported;

  private EntityCombinedView(View view, @Nullable ViewEntity viewEntity) {
    this.view = view;
    this.viewEntity = viewEntity;
    this.imported = false;
  }

  public static EntityCombinedView of(View view, ViewEntity viewEntity) {
    return new EntityCombinedView(view, viewEntity);
  }

  public static EntityCombinedView of(View view) {
    return new EntityCombinedView(view, null);
  }

  public EntityCombinedView withImported(boolean imported) {
    this.imported = imported;
    return this;
  }

  public EntityCombinedView withHiddenProperties(Set<String> hiddenProperties) {
    this.hiddenProperties = hiddenProperties == null ? Collections.emptySet() : hiddenProperties;
    return this;
  }

  @Override
  public Column[] columns() {
    return view.columns();
  }

  @Override
  public Representation[] representations() {
    return view.representations();
  }

  @Override
  public String defaultCatalog() {
    return view.defaultCatalog();
  }

  @Override
  public String defaultSchema() {
    return view.defaultSchema();
  }

  @Override
  public String name() {
    return view.name();
  }

  @Override
  public String comment() {
    return view.comment();
  }

  @Override
  public Map<String, String> properties() {
    Map<String, String> props = view.properties();
    if (props == null) {
      return Collections.emptyMap();
    }
    return props.entrySet().stream()
        .filter(p -> !hiddenProperties.contains(p.getKey()))
        .filter(entry -> entry.getKey() != null && entry.getValue() != null)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Audit auditInfo() {
    if (viewEntity == null) {
      return view.auditInfo();
    }
    AuditInfo mergedAudit =
        AuditInfo.builder()
            .withCreator(view.auditInfo().creator())
            .withCreateTime(view.auditInfo().createTime())
            .withLastModifier(view.auditInfo().lastModifier())
            .withLastModifiedTime(view.auditInfo().lastModifiedTime())
            .build();
    return mergedAudit.merge(viewEntity.auditInfo(), true /* overwrite */);
  }

  public boolean imported() {
    return imported;
  }

  public View viewFromCatalog() {
    return view;
  }

  /**
   * Returns the Gravitino-side view entity when this combined view was built from a full {@link
   * ViewEntity}, otherwise {@code null}.
   *
   * @return The view entity, or {@code null}.
   */
  @Nullable
  public ViewEntity viewEntity() {
    return viewEntity;
  }
}
