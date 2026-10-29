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

import org.apache.gravitino.annotation.Evolving;

/**
 * The closed set of concept types an {@link OntologyConcept} can have.
 *
 * <p>The set is closed by the Apache Ossie ontology specification, which allows exactly these two
 * values. A concept is therefore not a free-form kind: a business term with synonyms, an owner and
 * an approval state is not expressible as a concept type, which is why terms are separate objects
 * rather than concepts (see the design document).
 */
@Evolving
public enum ConceptType {

  /**
   * A concept whose instances are identifiable objects with identity, such as an account or a
   * customer.
   */
  ENTITY_TYPE,

  /**
   * A concept that represents a value rather than an identifiable object, such as an amount or a
   * currency code.
   */
  VALUE_TYPE;
}
