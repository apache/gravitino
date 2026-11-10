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

import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.SchemaEntityCleaner;
import org.apache.iceberg.catalog.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shared best-effort cleanup of Gravitino schema entities that become orphaned when an Iceberg
 * table or view drop empties their backing namespaces.
 */
final class IcebergOrphanSchemaCleanup {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergOrphanSchemaCleanup.class);

  private IcebergOrphanSchemaCleanup() {}

  /**
   * Removes Gravitino schema entities whose backing Iceberg namespace no longer exists after a
   * table or view drop.
   *
   * <p>Best-effort: the primary drop has already succeeded, so any failure here is logged and
   * swallowed rather than propagated to the caller.
   *
   * @param metalake the metalake the catalog belongs to
   * @param namespaceDispatcher dispatcher used to probe whether a namespace still exists in the
   *     catalog
   * @param context the Iceberg request context carrying the catalog name
   * @param namespace the namespace of the dropped table or view
   */
  static void bestEffortCleanUp(
      String metalake,
      IcebergNamespaceOperationDispatcher namespaceDispatcher,
      IcebergRequestContext context,
      Namespace namespace) {
    try {
      String separator = HierarchicalSchemaUtil.schemaSeparator();
      SchemaEntityCleaner.deleteOrphanedSchemaEntities(
          GravitinoEnv.getInstance().entityStore(),
          IcebergIdentifierUtils.toGravitinoSchemaIdentifier(
              metalake, context.catalogName(), namespace, separator),
          true,
          schemaIdent ->
              namespaceDispatcher.namespaceExists(
                  context,
                  Namespace.of(
                      HierarchicalSchemaUtil.splitSchemaName(schemaIdent.name(), separator))));
    } catch (RuntimeException e) {
      LOG.warn(
          "Failed to clean up orphaned Gravitino schema entities after the Iceberg backend operation "
              + "succeeded. catalog={}, namespace={}",
          context.catalogName(),
          namespace,
          e);
    }
  }
}
