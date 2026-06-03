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
package org.apache.gravitino.utils;

import static org.apache.gravitino.Entity.EntityType.SCHEMA;

import java.util.ArrayList;
import java.util.function.Predicate;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility for deleting schema entities whose backing catalog schemas no longer exist. */
public final class SchemaEntityCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaEntityCleaner.class);

  private SchemaEntityCleaner() {}

  /**
   * Deletes the outermost orphaned schema entity and its descendants from the store.
   *
   * <p>Candidates are checked from the starting schema toward the outermost ancestor. The first
   * candidate that still exists in the catalog stops the walk; every more-specific candidate
   * checked before that point is stale. A single cascade delete on the outermost stale schema
   * removes it and all descendant schema entities.
   *
   * <p>This is best-effort: callers invoke it after the primary drop has already succeeded, so any
   * failure (a store error or a catalog existence probe failure) is logged and swallowed rather
   * than propagated. The orphan, if any, is retried on a subsequent drop.
   *
   * @param store the Gravitino entity store; if {@code null}, this method is a no-op
   * @param schemaIdent the schema identifier to start checking from
   * @param includeSelf whether to check {@code schemaIdent} itself before checking ancestors
   * @param schemaExists predicate reporting whether a schema still exists in the underlying catalog
   */
  public static void deleteOrphanedSchemaEntities(
      EntityStore store,
      NameIdentifier schemaIdent,
      boolean includeSelf,
      Predicate<NameIdentifier> schemaExists) {
    if (store == null) {
      return;
    }

    try {
      String separator = HierarchicalSchemaUtil.schemaSeparator();
      ArrayList<String> schemaNames =
          new ArrayList<>(HierarchicalSchemaUtil.allScopes(schemaIdent.name(), separator));
      if (!includeSelf && !schemaNames.isEmpty()) {
        schemaNames.remove(0);
      }

      NameIdentifier outermostOrphan = null;
      for (String schemaName : schemaNames) {
        NameIdentifier candidate = NameIdentifier.of(schemaIdent.namespace(), schemaName);
        if (schemaExists.test(candidate)) {
          break;
        }
        outermostOrphan = candidate;
      }

      if (outermostOrphan == null) {
        return;
      }

      store.delete(outermostOrphan, SCHEMA, true);
    } catch (NoSuchEntityException e) {
      LOG.debug("The orphaned schema entity was already removed from the store", e);
    } catch (Exception e) {
      // Best-effort: the primary drop already succeeded, so swallow and log rather than fail it.
      LOG.warn("Failed to clean up orphaned schema entities starting from {}", schemaIdent, e);
    }
  }
}
