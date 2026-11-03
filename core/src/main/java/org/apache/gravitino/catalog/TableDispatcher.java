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
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;

/**
 * {@code TableDispatcher} interface acts as a specialization of the {@link TableCatalog} interface.
 * This interface is designed to potentially add custom behaviors or operations related to
 * dispatching or handling table-related events or actions that are not covered by the standard
 * {@code TableCatalog} operations.
 */
public interface TableDispatcher extends TableCatalog {
  /**
   * Imports an externally created table into Gravitino, or returns the already imported entity.
   * Catalog-reported audit fields take precedence over the supplied audit. Existing entity audits
   * are preserved. This operation does not create the table in the underlying catalog.
   *
   * @param ident the identifier of the existing external table
   * @param auditInfo fallback audit information used on initial import when the catalog reports no
   *     creator
   * @return the imported table
   * @throws NoSuchTableException if the external table does not exist
   */
  Table importTable(NameIdentifier ident, AuditInfo auditInfo) throws NoSuchTableException;
}
