/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Referred from Apache's connector/catalog implementation
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/TableCatalog.java

package com.datastrato.graviton.rel;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import java.util.Map;

/**
 * The TableCatalog interface defines the public API for managing tables in a schema. If the catalog
 * implementation supports tables, it must implement this interface.
 */
public interface TableCatalog {

  /**
   * List the tables in a namespace from the catalog.
   *
   * @param namespace a namespace
   * @return an array of table identifiers in the namespace
   * @throws NoSuchSchemaException if the schema does not exist
   */
  NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Load table metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident a table identifier
   * @return the table metadata
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(NameIdentifier ident) throws NoSuchTableException;

  /**
   * Check if a table exists using an {@link NameIdentifier} from the catalog.
   *
   * @param ident a table identifier
   * @return true if the table exists, false otherwise
   */
  default boolean tableExists(NameIdentifier ident) {
    try {
      return loadTable(ident) != null;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Create a table in the catalog.
   *
   * @param ident a table identifier
   * @param columns the columns of the new table
   * @param comment the table comment
   * @param properties the table properties
   * @return the created table metadata
   * @throws NoSuchSchemaException if the schema does not exist
   * @throws TableAlreadyExistsException if the table already exists
   */
  Table createTable(
      NameIdentifier ident, Column[] columns, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, TableAlreadyExistsException;

  /**
   * Apply the {@link TableChange change} to a table in the catalog.
   *
   * <p>Implementations may reject the change. If any change is rejected, no changes should be
   * applied to the table.
   *
   * @param ident a table identifier
   * @param changes table changes to apply to the table
   * @return the updated table metadata
   * @throws NoSuchTableException if the table does not exist
   * @throws IllegalArgumentException if the change is rejected by the implementation
   */
  Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException;

  /**
   * Drop a table from the catalog.
   *
   * @param ident a table identifier
   * @return true if the table was dropped, false if the table did not exist
   */
  boolean dropTable(NameIdentifier ident);

  /**
   * Drop a table from the catalog and completely remove its data.
   *
   * <p>If the catalog supports to purge a table, this method should be overridden. The default
   * implementation throws an {@link UnsupportedOperationException}.
   *
   * @param ident a table identifier
   * @return true if the table was purged, false if the table did not exist
   * @throws UnsupportedOperationException if the catalog does not support to purge a table
   */
  default boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }
}
