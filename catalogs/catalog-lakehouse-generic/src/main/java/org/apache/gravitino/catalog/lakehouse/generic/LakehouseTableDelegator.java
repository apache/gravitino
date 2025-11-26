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
package org.apache.gravitino.catalog.lakehouse.generic;

import java.util.List;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.storage.IdGenerator;

/**
 * A delegator interface for different lakehouse table formats to provide their specific table
 * operations and property definitions.
 */
public interface LakehouseTableDelegator {

  /**
   * Returns the table format name handled by this delegator.
   *
   * @return the table format name
   */
  String tableFormat();

  /**
   * Returns the list of property entries specific to the table format.
   *
   * @return the list of property entries
   */
  List<PropertyEntry<?>> tablePropertyEntries();

  /**
   * Create the managed table operations for the specific table format. This method should return a
   * new instance of {@link ManagedTableOperations} each time it is called. The returned instance
   * will be used in {@link GenericCatalogOperations} to perform the specific table operation.
   *
   * @param store the entity store
   * @param schemaOps the managed schema operations
   * @param idGenerator the ID generator
   * @return the managed table operations
   */
  ManagedTableOperations createTableOps(
      EntityStore store, ManagedSchemaOperations schemaOps, IdGenerator idGenerator);
}
