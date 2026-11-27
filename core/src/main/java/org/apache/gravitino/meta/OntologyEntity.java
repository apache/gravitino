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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityFieldLimits;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyDefinition;

/** Current snapshot of a stored ontology. */
public record OntologyEntity(
    Long id,
    String name,
    Namespace namespace,
    @Nullable String comment,
    OntologyDefinition definition,
    Map<String, String> properties,
    AuditInfo auditInfo)
    implements Ontology, Entity, HasIdentifier {

  private static final Field ID = Field.required("id", Long.class, "The stable ontology ID.");
  private static final Field NAME =
      Field.required("name", "The ontology name.", EntityFieldLimits.MAX_NAME_LENGTH);
  private static final Field COMMENT =
      Field.optional("comment", "The ontology comment.", EntityFieldLimits.MAX_COMMENT_LENGTH);
  private static final Field DEFINITION =
      Field.required("definition", OntologyDefinition.class, "The ontology definition.");
  private static final Field PROPERTIES =
      Field.required("properties", Map.class, "The ontology properties.");
  private static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit information.");

  /**
   * Creates a current ontology snapshot.
   *
   * @param id The stable ontology ID.
   * @param name The current name.
   * @param namespace The ontology namespace.
   * @param comment The optional comment.
   * @param definition The current definition.
   * @param properties The Gravitino properties.
   * @param auditInfo The current audit information.
   */
  public OntologyEntity {
    properties = ImmutableMap.copyOf(properties);
  }

  /** {@inheritDoc} */
  @Override
  public EntityType type() {
    return EntityType.ONTOLOGY;
  }

  /** {@inheritDoc} */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> values = new HashMap<>();
    values.put(ID, id);
    values.put(NAME, name);
    values.put(COMMENT, comment);
    values.put(DEFINITION, definition);
    values.put(PROPERTIES, properties);
    values.put(AUDIT_INFO, auditInfo);
    return values;
  }
}
