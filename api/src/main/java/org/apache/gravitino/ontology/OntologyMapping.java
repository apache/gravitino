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
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;

/** A semantic-model-scoped binding between a model and a metalake ontology. */
@Evolving
public interface OntologyMapping extends Auditable {

  /**
   * @return The current mapping name.
   */
  String name();

  /**
   * @return The mapping description, if present.
   */
  @Nullable
  String comment();

  /**
   * @return The current name of the referenced ontology.
   */
  String ontology();

  /**
   * @return The current mapping definition.
   */
  OntologyMappingDefinition definition();

  /**
   * @return The mapping's immutable Gravitino properties.
   */
  Map<String, String> properties();
}
