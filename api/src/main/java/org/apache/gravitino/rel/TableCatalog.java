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

package org.apache.gravitino.rel;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;

/**
 * The TableCatalog interface defines the public API for managing tables in a schema. If the catalog
 * implementation supports tables, it must implement this interface.
 */
@Evolving
public interface TableCatalog {

  /**
   * List the tables in a namespace from the catalog.
   *
   * @param namespace A namespace.
   * @return An array of table identifiers in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException;

  /**
   * Load table metadata by {@link NameIdentifier} from the catalog.
   *
   * @param ident A table identifier.
   * @return The table metadata.
   * @throws NoSuchTableException If the table does not exist.
   */
  Table loadTable(NameIdentifier ident) throws NoSuchTableException;

  /**
   * Check if a table exists using an {@link NameIdentifier} from the catalog.
   *
   * @param ident A table identifier.
   * @return true If the table exists, false otherwise.
   */
  default boolean tableExists(NameIdentifier ident) {
    try {
      return loadTable(ident) != null;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Create a table in the catalog based on the provided details.
   *
   * <p>This method returns the table as defined by the user without applying all defaults. If you
   * need the table with default values applied, use the {@link #loadTable(NameIdentifier)} method
   * after creation.
   *
   * @param ident A table identifier.
   * @param columns The columns of the new table.
   * @param comment The table comment.
   * @param properties The table properties.
   * @return The table as defined by the caller, without all default values.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws TableAlreadyExistsException If the table already exists.
   */
  default Table createTable(
      NameIdentifier ident, Column[] columns, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    return createTable(
        ident,
        columns,
        comment,
        properties,
        Transforms.EMPTY_TRANSFORM,
        Distributions.NONE,
        new SortOrder[0]);
  }

  /**
   * Create a table in the catalog.
   *
   * @param ident A table identifier.
   * @param columns The columns of the new table.
   * @param comment The table comment.
   * @param properties The table properties.
   * @param partitions The table partitioning.
   * @return The created table metadata.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws TableAlreadyExistsException If the table already exists.
   */
  default Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    return createTable(
        ident, columns, comment, properties, partitions, Distributions.NONE, new SortOrder[0]);
  }

  /**
   * Create a table in the catalog.
   *
   * @param ident A table identifier.
   * @param columns The columns of the new table.
   * @param comment The table comment.
   * @param properties The table properties.
   * @param distribution The distribution of the table
   * @param sortOrders The sort orders of the table
   * @return The created table metadata.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws TableAlreadyExistsException If the table already exists.
   */
  default Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Distribution distribution,
      SortOrder[] sortOrders)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    return createTable(
        ident, columns, comment, properties, new Transform[0], distribution, sortOrders);
  }

  /**
   * Create a table in the catalog.
   *
   * @param ident A table identifier.
   * @param columns The columns of the new table.
   * @param comment The table comment.
   * @param properties The table properties.
   * @param distribution The distribution of the table.
   * @return The created table metadata.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws TableAlreadyExistsException If the table already exists.
   */
  default Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Distribution distribution)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    return createTable(
        ident, columns, comment, properties, new Transform[0], distribution, new SortOrder[0]);
  }

  /**
   * Create a table in the catalog.
   *
   * @param ident A table identifier.
   * @param columns The columns of the new table.
   * @param comment The table comment.
   * @param properties The table properties.
   * @param sortOrders The sort orders of the table
   * @return The created table metadata.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws TableAlreadyExistsException If the table already exists.
   */
  default Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      SortOrder[] sortOrders)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    return createTable(
        ident, columns, comment, properties, new Transform[0], Distributions.NONE, sortOrders);
  }

  /**
   * Create a partitioned table in the catalog.
   *
   * @param ident A table identifier.
   * @param columns The columns of the new table.
   * @param comment The table comment.
   * @param properties The table properties.
   * @param distribution The distribution of the table
   * @param sortOrders The sort orders of the table
   * @param partitions The table partitioning.
   * @return The created table metadata.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws TableAlreadyExistsException If the table already exists.
   */
  default Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    return createTable(
        ident,
        columns,
        comment,
        properties,
        partitions,
        distribution,
        sortOrders,
        Indexes.EMPTY_INDEXES);
  }

  /**
   * Create a table in the catalog.
   *
   * @param ident A table identifier.
   * @param columns The columns of the new table.
   * @param comment The table comment.
   * @param properties The table properties.
   * @param distribution The distribution of the table
   * @param sortOrders The sort orders of the table
   * @param partitions The table partitioning.
   * @param indexes The table indexes.
   * @return The created table metadata.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws TableAlreadyExistsException If the table already exists.
   */
  Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException;

  /**
   * Apply the {@link TableChange change} to a table in the catalog.
   *
   * <p>Implementations may reject the change. If any change is rejected, no changes should be
   * applied to the table.
   *
   * @param ident A table identifier.
   * @param changes Table changes to apply to the table.
   * @return The updated table metadata.
   * @throws NoSuchTableException If the table does not exist.
   * @throws IllegalArgumentException If the change is rejected by the implementation.
   */
  Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException;

  /**
   * Removes both the metadata and the directory associated with the table from the file system if
   * the table is not an external table. In case of an external table, only the associated metadata
   * is removed.
   *
   * @param ident A table identifier.
   * @return True if the table is dropped, false if the table does not exist.
   */
  boolean dropTable(NameIdentifier ident);

  /**
   * Drop a table from the catalog and completely remove its data. Removes both the metadata and the
   * directory associated with the table completely and skipping trash. If the table is an external
   * table or the catalogs don't support purge table, {@link UnsupportedOperationException} is
   * thrown.
   *
   * <p>If the catalog supports to purge a table, this method should be overridden. The default
   * implementation throws an {@link UnsupportedOperationException}.
   *
   * @param ident A table identifier.
   * @return True if the table is purged, false if the table does not exist.
   * @throws UnsupportedOperationException If the catalog does not support to purge a table.
   */
  default boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    throw new UnsupportedOperationException("purgeTable not supported.");
  }
}
