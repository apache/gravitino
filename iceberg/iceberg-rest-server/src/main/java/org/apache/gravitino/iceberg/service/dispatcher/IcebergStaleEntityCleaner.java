/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.dispatcher;

import java.io.IOException;
import java.util.Collections;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes Gravitino state left under a table or view name by an earlier object, before a freshly
 * created object with that name is imported.
 *
 * <p>Iceberg REST tables and views carry no Gravitino id, so the core import treats any entity
 * stored under the name as the object itself. Once the Iceberg backend has just created or
 * registered an object without overwrite, no object had that name, so any such entity is stale. If
 * it were kept, the new object would inherit the old object's id and with it every role grant, tag,
 * policy and owner bound to it. Deleting the entity cascades those bindings, and removing the
 * authorization plugin privileges clears the policies the plugin keeps by name.
 */
final class IcebergStaleEntityCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergStaleEntityCleaner.class);

  private IcebergStaleEntityCleaner() {}

  /**
   * Removes the stale table entity and plugin privileges left under a freshly created table name.
   *
   * @param metalake the metalake name
   * @param catalogName the catalog name
   * @param tableIdentifier the Iceberg identifier of the newly created table
   */
  static void removeStaleTable(
      String metalake, String catalogName, TableIdentifier tableIdentifier) {
    removeStale(metalake, catalogName, tableIdentifier, Entity.EntityType.TABLE);
  }

  /**
   * Removes the stale view entity and plugin privileges left under a freshly created view name.
   *
   * @param metalake the metalake name
   * @param catalogName the catalog name
   * @param viewIdentifier the Iceberg identifier of the newly created view
   */
  static void removeStaleView(String metalake, String catalogName, TableIdentifier viewIdentifier) {
    removeStale(metalake, catalogName, viewIdentifier, Entity.EntityType.VIEW);
  }

  private static void removeStale(
      String metalake, String catalogName, TableIdentifier identifier, Entity.EntityType type) {
    NameIdentifier ident =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(
            metalake, catalogName, identifier, HierarchicalSchemaUtil.schemaSeparator());
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    if (store != null) {
      try {
        if (store.delete(ident, type)) {
          LOG.warn(
              "Removed a stale {} entity left under {} by an earlier object before importing the"
                  + " newly created one",
              type,
              ident);
        }
      } catch (NoSuchEntityException ignore) {
        // No entity under the name: nothing is stale.
      } catch (IOException ioe) {
        // Fail rather than import: the import would adopt the stale entity and its bindings.
        throw new RuntimeException(
            String.format("Failed to remove the stale %s entity %s", type, ident), ioe);
      }
    }

    // The plugin keeps policies by name, which can outlive the entity. The old object's storage
    // locations are unknown here, so only the name-based policies are removed.
    AuthorizationUtils.authorizationPluginRemovePrivileges(ident, type, Collections.emptyList());
  }
}
