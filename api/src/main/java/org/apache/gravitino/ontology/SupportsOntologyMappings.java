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
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;

/** Manages mappings in a semantic model. */
@Evolving
public interface SupportsOntologyMappings {
  /**
   * @return The names of the visible mappings.
   */
  String[] listOntologyMappings();

  /**
   * Loads the current mapping.
   *
   * @param name The mapping name.
   * @return The current mapping.
   */
  OntologyMapping loadOntologyMapping(String name);

  /**
   * Creates a new mapping.
   *
   * @param name The new mapping name.
   * @param comment The optional description.
   * @param ontology The ontology name in the same metalake.
   * @param definition The mapping definition.
   * @param properties The Gravitino properties.
   * @return The created mapping.
   */
  OntologyMapping createOntologyMapping(
      String name,
      @Nullable String comment,
      String ontology,
      OntologyMappingDefinition definition,
      Map<String, String> properties);

  /**
   * Imports an Ossie document and creates a mapping.
   *
   * @param document The Ossie document.
   * @return The imported mapping.
   */
  OntologyMapping importOntologyMapping(OssieDocument document);

  /**
   * Exports the current mapping as an Ossie document.
   *
   * @param name The mapping name.
   * @param format The requested document format.
   * @return The Ossie document.
   */
  OssieDocument exportOntologyMapping(String name, OssieFormat format);

  /**
   * Applies changes to a mapping in order and atomically.
   *
   * @param name The mapping name.
   * @param changes The ordered changes.
   * @return The updated mapping.
   */
  OntologyMapping alterOntologyMapping(String name, OntologyMappingChange... changes);

  /**
   * Drops a mapping.
   *
   * @param name The mapping name.
   * @return Whether it existed and was dropped.
   */
  boolean dropOntologyMapping(String name);
}
