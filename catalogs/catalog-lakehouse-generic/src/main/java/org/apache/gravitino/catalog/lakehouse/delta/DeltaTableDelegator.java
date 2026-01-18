/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.lakehouse.delta;

import static org.apache.gravitino.catalog.lakehouse.delta.DeltaConstants.DELTA_TABLE_FORMAT;

import java.util.Collections;
import java.util.List;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.catalog.lakehouse.generic.LakehouseTableDelegator;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.storage.IdGenerator;

/**
 * Delegator for Delta Lake table format in Gravitino lakehouse catalog.
 *
 * <p>This delegator provides table operations specific to external Delta Lake tables. It enables
 * Gravitino to register and manage metadata for existing Delta tables without creating or modifying
 * the underlying Delta table data.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Supports external Delta table registration (requires {@code external=true} property)
 *   <li>Schema is provided by user in CREATE TABLE request (not read from Delta log)
 *   <li>Uses standard table properties ({@code location}, {@code external}) - no Delta-specific
 *       properties needed
 *   <li>Drop operations only remove metadata, preserving the Delta table data
 *   <li>No ALTER TABLE or PURGE support (external tables should be modified directly)
 * </ul>
 */
public class DeltaTableDelegator implements LakehouseTableDelegator {

  @Override
  public String tableFormat() {
    return DELTA_TABLE_FORMAT;
  }

  /**
   * Returns Delta-specific table property entries.
   *
   * <p>Delta tables use standard table properties ({@link Table#PROPERTY_LOCATION} and {@link
   * Table#PROPERTY_EXTERNAL}), so no Delta-specific properties are needed.
   *
   * @return an empty list since all required properties are standard table properties
   */
  @Override
  public List<PropertyEntry<?>> tablePropertyEntries() {
    return Collections.emptyList();
  }

  @Override
  public ManagedTableOperations createTableOps(
      EntityStore store, ManagedSchemaOperations schemaOps, IdGenerator idGenerator) {
    return new DeltaTableOperations(store, schemaOps, idGenerator);
  }
}
