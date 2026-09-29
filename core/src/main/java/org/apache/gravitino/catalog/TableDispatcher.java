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
import org.apache.gravitino.connector.SupportsLightTableLoad;
import org.apache.gravitino.exceptions.NoSuchTableException;
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
   * Loads a table without contacting the storage system that holds it, for callers that do not need
   * a fresh schema. See {@link SupportsLightTableLoad} for the guarantees this trades away and for
   * when to prefer it over {@link #loadTable}.
   *
   * <p>The default falls back to {@link #loadTable}, which is always a correct answer because a
   * light load may return everything a full load returns. Implementations that sit in front of
   * another dispatcher should override this to forward it, so that a connector able to serve the
   * light load is actually reached.
   *
   * @param ident the identifier of the table to load.
   * @return the loaded table.
   * @throws NoSuchTableException if the table does not exist.
   */
  default Table loadTableLight(NameIdentifier ident) throws NoSuchTableException {
    return loadTable(ident);
  }
}
