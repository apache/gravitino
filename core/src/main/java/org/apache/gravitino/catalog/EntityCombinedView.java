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
import org.apache.gravitino.Audit;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.rel.View;

/**
 * A View class to represent a view metadata object that combines the metadata both from {@link
 * View} and {@link GenericEntity}.
 */
public final class EntityCombinedView implements View {

  private final View view;

  private final GenericEntity viewEntity;

  // Field "imported" is used to indicate whether the entity has been imported to Gravitino
  // managed storage backend. If "imported" is true, it means that storage backend have stored
  // the correct entity. Otherwise, we should import the external entity to the storage backend.
  private boolean imported;

  private EntityCombinedView(View view, GenericEntity viewEntity) {
    this.view = view;
    this.viewEntity = viewEntity;
    this.imported = false;
  }

  public static EntityCombinedView of(View view, GenericEntity viewEntity) {
    return new EntityCombinedView(view, viewEntity);
  }

  public static EntityCombinedView of(View view) {
    return new EntityCombinedView(view, null);
  }

  public EntityCombinedView withImported(boolean imported) {
    this.imported = imported;
    return this;
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
    return view.properties();
  }

  @Override
  public Audit auditInfo() {
    return view.auditInfo();
  }

  public boolean imported() {
    return imported;
  }

  public View viewFromCatalog() {
    return view;
  }

  public GenericEntity viewFromGravitino() {
    return viewEntity;
  }
}
