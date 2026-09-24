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
import java.util.Set;

/**
 * An optional capability for deleting a catalog after checking its remaining schemas atomically.
 */
public interface SupportsConditionalCatalogDelete {

  /**
   * Delete a catalog only if all remaining schemas have IDs in the allowlist. The check and
   * deletion must run in one transaction and be serialized with schema creation.
   *
   * @param ident the catalog identifier
   * @param allowedSchemaIds IDs of schemas that may be deleted with the catalog
   * @return true if the catalog was deleted
   * @throws IOException if the store operation fails
   */
  boolean deleteCatalogWithAllowedSchemas(NameIdentifier ident, Set<Long> allowedSchemaIds)
      throws IOException;
}
