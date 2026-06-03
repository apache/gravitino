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

package org.apache.gravitino.iceberg.service.dispatcher;

import java.util.Optional;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.cleanup.IcebergCleanupManager;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Request-path helpers shared by the table and namespace executors for async table purge. */
final class IcebergCleanupHelper {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergCleanupHelper.class);

  private IcebergCleanupHelper() {}

  /**
   * Returns the catalog entity id for {@code catalogName}. The catalog is already loaded for the
   * current request, so this reads its in-memory entity without an extra entity-store lookup.
   */
  static long catalogId(String catalogName) {
    String metalake = IcebergRESTServerContext.getInstance().metalakeName();
    return GravitinoEnv.getInstance()
        .catalogManager()
        .loadCatalogAndWrap(NameIdentifier.of(metalake, catalogName))
        .catalog()
        .entity()
        .id();
  }

  /**
   * Fails a create or register with {@code 409} while a cleanup job still holds the identifier.
   * Reusing the name before its files are gone would let the new table share the old table's
   * storage prefix. A name with no resolvable catalog entity cannot have a cleanup job, so it stays
   * usable.
   */
  static void rejectIfBeingPurged(
      Optional<IcebergCleanupManager> cleanupManager,
      String catalogName,
      Namespace namespace,
      String tableName) {
    if (cleanupManager.isEmpty()) {
      return;
    }
    long catalogId;
    try {
      catalogId = catalogId(catalogName);
    } catch (RuntimeException e) {
      LOG.warn("No catalog id for {}; skipping purge check", catalogName, e);
      return;
    }
    if (cleanupManager.get().isNameOccupied(catalogId, namespace.toString(), tableName)) {
      throw new AlreadyExistsException(
          "Table %s.%s is being purged; retry after cleanup completes", namespace, tableName);
    }
  }
}
