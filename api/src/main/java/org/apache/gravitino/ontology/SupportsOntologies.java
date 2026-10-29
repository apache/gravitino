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
 * Interface for metalakes that support ontologies.
 *
 * <p>An ontology is metalake-scoped, so every method addresses an ontology by name alone and the
 * metalake makes up the rest of the address. This interface is mixed into a metalake, like {@code
 * SupportsTags} and {@code SupportsPolicies}, so a caller that can reach a metalake can manage its
 * ontologies without knowing the REST layout.
 */
@Evolving
public interface SupportsOntologies extends OntologyOperations {}