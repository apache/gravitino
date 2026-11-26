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
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.tag.SupportsTags;

/** A metalake-scoped vocabulary of concepts and relationships. */
@Evolving
public interface Ontology extends Auditable {

  /**
   * @return The current ontology name.
   */
  String name();

  /**
   * @return The ontology description, if present.
   */
  @Nullable
  String comment();

  /**
   * @return The current ontology definition.
   */
  OntologyDefinition definition();

  /**
   * @return The ontology's immutable Gravitino properties.
   */
  Map<String, String> properties();

  /**
   * @return Tag operations for this ontology.
   */
  default SupportsTags supportsTags() {
    throw new UnsupportedOperationException("This ontology does not support tags.");
  }

  /**
   * @return Policy operations for this ontology.
   */
  default SupportsPolicies supportsPolicies() {
    throw new UnsupportedOperationException("This ontology does not support policies.");
  }
}
