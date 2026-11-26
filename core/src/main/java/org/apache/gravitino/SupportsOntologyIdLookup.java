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
package org.apache.gravitino;

import java.io.IOException;
import org.apache.gravitino.exceptions.NoSuchEntityException;

/** Optional store capability for resolving ontology IDs without loading their definitions. */
public interface SupportsOntologyIdLookup {

  /**
   * Resolves a live ontology ID.
   *
   * @param identifier the ontology identifier in its metalake's virtual namespace
   * @return the active ontology ID
   * @throws NoSuchEntityException if the ontology or its metalake does not exist
   * @throws IOException if the lookup fails
   */
  long getOntologyId(NameIdentifier identifier) throws IOException;
}
