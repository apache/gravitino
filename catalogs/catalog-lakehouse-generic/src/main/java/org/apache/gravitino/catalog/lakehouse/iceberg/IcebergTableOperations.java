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
package org.apache.gravitino.catalog.lakehouse.iceberg;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
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
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.storage.IdGenerator;

public class IcebergTableOperations extends ManagedTableOperations {

  private final EntityStore store;

  private final ManagedSchemaOperations schemaOps;

  private final IdGenerator idGenerator;

  public IcebergTableOperations(
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
    String format = properties.get(Table.PROPERTY_TABLE_FORMAT);
    Preconditions.checkArgument(
        format != null, "Table format must be specified for Iceberg tables");
    Preconditions.checkArgument(
        IcebergTableDelegator.ICEBERG_TABLE_FORMAT.equalsIgnoreCase(format),
        "Unsupported table format for Iceberg table operations: %s",
        format);

    boolean external =
        Optional.ofNullable(properties.get(Table.PROPERTY_EXTERNAL))
            .map(Boolean::parseBoolean)
            .orElse(false);
    Preconditions.checkArgument(external, "Iceberg table in generic catalog must be external");

    return super.createTable(
        ident, columns, comment, properties, partitions, distribution, sortOrders, indexes);
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    validateImmutableProperties(changes);
    return super.alterTable(ident, changes);
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) {
    throw new UnsupportedOperationException("Purge is not supported for external Iceberg tables");
  }

  private void validateImmutableProperties(TableChange[] changes) {
    for (TableChange change : changes) {
      if (change instanceof TableChange.SetProperty setProperty) {
        validateImmutableProperty(setProperty.getProperty());
      } else if (change instanceof TableChange.RemoveProperty removeProperty) {
        validateImmutableProperty(removeProperty.getProperty());
      }
    }
  }

  private void validateImmutableProperty(String property) {
    if (Table.PROPERTY_TABLE_FORMAT.equalsIgnoreCase(property)
        || Table.PROPERTY_EXTERNAL.equalsIgnoreCase(property)) {
      throw new IllegalArgumentException(
          String.format("Property %s is immutable for Iceberg tables", property));
    }
  }
}
