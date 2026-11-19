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
package org.apache.gravitino.meta;

import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;

/** A current model identifier and its matching datasets, without loading its definition. */
public final class SemanticModelDatasetListing implements Entity, HasIdentifier {
  private final Long id;
  private final NameIdentifier identifier;
  private final List<String> datasets;

  /**
   * Creates a relation-query result.
   *
   * @param id The containing model ID.
   * @param identifier The current fully qualified model identifier.
   * @param datasets Matching exact dataset names.
   */
  public SemanticModelDatasetListing(Long id, NameIdentifier identifier, List<String> datasets) {
    this.id = id;
    this.identifier = identifier;
    this.datasets = List.copyOf(datasets);
  }

  /**
   * @return Matching dataset names in the current definition.
   */
  public List<String> datasets() {
    return datasets;
  }

  @Override
  public Long id() {
    return id;
  }

  @Override
  public String name() {
    return identifier.name();
  }

  @Override
  public Namespace namespace() {
    return identifier.namespace();
  }

  @Override
  public EntityType type() {
    return EntityType.SEMANTIC_MODEL;
  }

  @Override
  public Map<Field, Object> fields() {
    return Map.of(
        SemanticModelEntity.ID,
        id,
        SemanticModelEntity.NAME,
        name(),
        SemanticModelEntity.NAMESPACE,
        namespace());
  }
}
