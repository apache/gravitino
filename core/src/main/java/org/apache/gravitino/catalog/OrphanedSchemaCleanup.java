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
package org.apache.gravitino.catalog;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.utils.SchemaEntityCleaner;

/**
 * Shared orphaned-schema cleanup for the core operation dispatchers.
 *
 * <p>Both the table and view drop paths need to remove Gravitino schema entities whose backing
 * catalog schema no longer exists. The existence probe goes through {@link
 * OperationDispatcher#doWithCatalog} against the {@link OperationDispatcher#store}, so this helper
 * lives in the same package to reuse those {@code protected} members and keep the logic in one
 * place.
 */
final class OrphanedSchemaCleanup {

  private OrphanedSchemaCleanup() {}

  /**
   * Removes Gravitino schema entities whose backing catalog schema no longer exists, starting from
   * {@code schemaIdentifier} and walking toward its outermost ancestor.
   *
   * @param dispatcher the dispatcher whose entity store and catalog access are used for cleanup
   * @param catalogIdent the identifier of the catalog owning the schema
   * @param schemaIdentifier the schema identifier to start the orphan check from
   */
  static void cleanUp(
      OperationDispatcher dispatcher,
      NameIdentifier catalogIdent,
      NameIdentifier schemaIdentifier) {
    SchemaEntityCleaner.deleteOrphanedSchemaEntities(
        dispatcher.store,
        schemaIdentifier,
        true,
        schemaIdent ->
            dispatcher.doWithCatalog(
                catalogIdent,
                c -> c.doWithSchemaOps(s -> s.schemaExists(schemaIdent)),
                RuntimeException.class));
  }
}
