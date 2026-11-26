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
package org.apache.gravitino.ontology;

import java.util.Map;
import org.apache.gravitino.annotation.Evolving;

/**
 * The immutable, JSON-compatible content of an ontology. Field names match the Gravitino REST
 * definition, including {@code ontology}.
 */
@Evolving
public final class OntologyDefinition {

  private final Map<String, Object> fields;

  private OntologyDefinition(Map<String, ?> fields) {
    this.fields = DefinitionFields.copy(fields);
  }

  /**
   * Creates a definition from its native, camel-case fields.
   *
   * @param fields The definition fields.
   * @return The immutable definition.
   */
  public static OntologyDefinition of(Map<String, ?> fields) {
    return new OntologyDefinition(fields);
  }

  /**
   * @return An immutable view of the definition fields.
   */
  public Map<String, Object> fields() {
    return fields;
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    return obj instanceof OntologyDefinition && fields.equals(((OntologyDefinition) obj).fields);
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return fields.hashCode();
  }
  /** {@inheritDoc} */
  @Override
  public String toString() {
    return fields.toString();
  }
}
