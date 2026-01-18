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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.storage.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Table operations for Delta Lake tables in Gravitino lakehouse catalog.
 *
 * <p>This class handles the lifecycle of external Delta Lake table metadata in Gravitino. It
 * focuses on metadata management only and does not interact with the actual Delta table data or
 * Delta log files.
 *
 * <p><b>Supported Operations:</b>
 *
 * <ul>
 *   <li><b>Create Table:</b> Registers an external Delta table by storing its schema and location
 *       in Gravitino's metadata store. Requires {@code external=true} property.
 *   <li><b>Load Table:</b> Retrieves table metadata from Gravitino's metadata store
 *   <li><b>Drop Table:</b> Removes metadata only, preserving the physical Delta table data
 * </ul>
 *
 * <p><b>Unsupported Operations:</b>
 *
 * <ul>
 *   <li><b>Alter Table:</b> Not supported; users should modify the Delta table directly using Delta
 *       Lake APIs, then optionally recreate the catalog entry with updated schema
 *   <li><b>Purge Table:</b> Not supported for external tables; data lifecycle is managed externally
 * </ul>
 *
 * <p><b>Design Decisions:</b>
 *
 * <ul>
 *   <li>Only supports external tables ({@code external=true} must be explicitly set)
 *   <li>Schema comes from CREATE TABLE request (not validated against Delta log)
 *   <li>User is responsible for ensuring schema accuracy matches the actual Delta table
 *   <li>Partitions, distribution, sort orders, and indexes are ignored with warnings (not errors)
 * </ul>
 */
public class DeltaTableOperations extends ManagedTableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(DeltaTableOperations.class);

  private final EntityStore store;
  private final ManagedSchemaOperations schemaOps;
  private final IdGenerator idGenerator;

  public DeltaTableOperations(
      EntityStore store, ManagedSchemaOperations schemaOps, IdGenerator idGenerator) {
    this.store = store;
    this.schemaOps = schemaOps;
    this.idGenerator = idGenerator;
  }

  @Override
  protected EntityStore store() {
    return store;
  }

  @Override
  protected SupportsSchemas schemas() {
    return schemaOps;
  }

  @Override
  protected IdGenerator idGenerator() {
    return idGenerator;
  }

  /**
   * Creates an external Delta table by registering its metadata in Gravitino.
   *
   * <p>This method validates that the table is explicitly marked as external and has a valid
   * location, then stores the metadata in Gravitino's entity store. It does not create or modify
   * the actual Delta table data.
   *
   * <p><b>Required Properties:</b>
   *
   * <ul>
   *   <li>{@code external=true} - Must be explicitly set to create external Delta tables
   *   <li>{@code location} - Storage path of the existing Delta table
   * </ul>
   *
   * <p><b>Ignored Parameters (with warnings):</b>
   *
   * <ul>
   *   <li>Partitions - Delta table partitioning is managed in the Delta log
   *   <li>Distribution - Not applicable for external Delta tables
   *   <li>Sort orders - Not applicable for external Delta tables
   *   <li>Indexes - Not applicable for external Delta tables
   * </ul>
   *
   * @param ident the table identifier
   * @param columns the table columns (schema provided by user)
   * @param comment the table comment
   * @param properties the table properties (must include {@code external=true} and {@code
   *     location})
   * @param partitions the partitioning (ignored with warning)
   * @param distribution the distribution (ignored with warning)
   * @param sortOrders the sort orders (ignored with warning)
   * @param indexes the indexes (ignored with warning)
   * @return the created table metadata
   * @throws NoSuchSchemaException if the schema does not exist
   * @throws TableAlreadyExistsException if the table already exists
   * @throws IllegalArgumentException if {@code external=true} is not set or location is missing
   */
  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    Map<String, String> copiedProperties = properties == null ? Collections.emptyMap() : properties;

    // Validate that the table is explicitly marked as external
    Preconditions.checkArgument(
        copiedProperties.containsKey(Table.PROPERTY_EXTERNAL)
            && "true".equalsIgnoreCase(copiedProperties.get(Table.PROPERTY_EXTERNAL)),
        "Gravitino only supports creating external Delta tables"
            + " for now. Please set property 'external=true' when creating Delta tables.");

    // Validate required location property
    String location = copiedProperties.get(Table.PROPERTY_LOCATION);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(location),
        "Property '%s' is required for external Delta tables. Please specify the"
            + " Delta table location.",
        Table.PROPERTY_LOCATION);

    // Warn about ignored parameters (don't fail - be permissive)
    if (partitions != null && partitions.length != 0) {
      LOG.warn(
          "Delta table doesn't support specifying partitioning in CREATE TABLE, will ignore"
              + " this field.");
    }

    if (distribution != null && !distribution.equals(Distributions.NONE)) {
      LOG.warn(
          "Delta table doesn't support specifying distribution in CREATE TABLE, will ignore"
              + " this field.");
    }

    if (sortOrders != null && sortOrders.length != 0) {
      LOG.warn(
          "Delta table doesn't support specifying sort orders in CREATE TABLE, will ignore"
              + " this field.");
    }

    if (indexes != null && indexes.length != 0) {
      LOG.warn(
          "Delta table doesn't support specifying indexes in CREATE TABLE, will ignore"
              + " this field.");
    }

    // Store metadata in entity store (schema from user request)
    return super.createTable(
        ident, columns, comment, copiedProperties, partitions, distribution, sortOrders, indexes);
  }

  /**
   * Alters a Delta table.
   *
   * <p>This operation is not supported for external Delta tables. Users should modify the Delta
   * table directly using Delta Lake APIs or tools. If the schema changes, the table entry in
   * Gravitino can be dropped and recreated with the updated schema.
   *
   * @param ident the table identifier
   * @param changes the table changes to apply
   * @return never returns (always throws exception)
   * @throws UnsupportedOperationException always thrown as ALTER is not supported
   */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException(
        "ALTER TABLE operations are not supported for external Delta tables. "
            + "Please modify the Delta table directly using Delta Lake APIs or tools, "
            + "then DROP and recreate the table entry in Gravitino with the updated schema if"
            + " needed.");
  }

  /**
   * Purges a Delta table (removes both metadata and data).
   *
   * <p>This operation is not supported for external Delta tables. External table data is managed
   * outside of Gravitino, so purging is not applicable. Use {@link #dropTable(NameIdentifier)} to
   * remove only the metadata, leaving the Delta table data intact.
   *
   * @param ident the table identifier
   * @return never returns (always throws exception)
   * @throws UnsupportedOperationException always thrown as PURGE is not supported for external
   *     tables
   */
  @Override
  public boolean purgeTable(NameIdentifier ident) {
    throw new UnsupportedOperationException(
        "Purge operation is not supported for external Delta tables. "
            + "External table data is managed outside of Gravitino. "
            + "Use dropTable() to remove metadata only, preserving the Delta table data.");
  }
}
