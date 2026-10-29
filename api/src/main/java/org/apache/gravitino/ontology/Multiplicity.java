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
 * The cardinality of an {@link OntologyRelationship}, from the concept that owns the relationship
 * to the concept it points at.
 *
 * <p>The set is closed by the Apache Ossie ontology specification. An omitted multiplicity means
 * the relationship is unconstrained; Gravitino does not default it.
 */
@Evolving
public enum Multiplicity {

  /** Many instances of the owning concept refer to one instance of the related concept. */
  MANY_TO_ONE,

  /** Each instance of the owning concept refers to exactly one instance of the related concept. */
  ONE_TO_ONE;
}
