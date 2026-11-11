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

package org.apache.gravitino.semantic;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;

/** Source-scoped discovery of datasets contained in Semantic Models. */
@Evolving
public interface SupportsSemanticModels {
  /**
   * Lists datasets directly associated with this source from models the caller can load.
   *
   * <p>Each identifier has namespace {@code [metalake, catalog, schema, semanticModel]} and the
   * exact dataset name, which may contain literal dots. Results may span catalogs in the source's
   * metalake. Results are sorted lexicographically by namespace components, then by dataset name,
   * using case-sensitive {@link String#compareTo(String)} order for each component.
   *
   * <p>For a returned {@code dataset}, {@code NameIdentifier.of(dataset.namespace().levels())}
   * identifies its containing model.
   *
   * <p>Associations record the sources resolved at the latest definition write. They do not
   * establish whether a model can currently execute, and do not include transitive dependencies.
   *
   * @return Fully qualified dataset identifiers, or an empty array when no matches are visible.
   * @throws NoSuchMetadataObjectException If the source does not exist.
   */
  NameIdentifier[] listSemanticModelDatasets() throws NoSuchMetadataObjectException;
}
