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
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.catalog.TableFormatCapability;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
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
  public Set<TableFormatCapability> capabilities() {
    return EnumSet.of(
        TableFormatCapability.SUPPORTS_PARTITIONING,
        TableFormatCapability.SUPPORTS_DISTRIBUTION,
        TableFormatCapability.SUPPORTS_SORT_ORDERS);
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
    boolean external =
        Boolean.parseBoolean(properties.getOrDefault(Table.PROPERTY_EXTERNAL, "false"));
    Preconditions.checkArgument(external, "Iceberg table in generic catalog must be external");

    if (Distributions.NONE.equals(distribution) || distribution == null) {
      int sortOrderCount = sortOrders == null ? 0 : sortOrders.length;
      int partitionCount = partitions == null ? 0 : partitions.length;
      distribution = getIcebergDefaultDistribution(sortOrderCount > 0, partitionCount > 0);
    }

    return super.createTable(
        ident, columns, comment, properties, partitions, distribution, sortOrders, indexes);
  }

  @Override
  public boolean purgeTable(NameIdentifier ident) {
    throw new UnsupportedOperationException("Purge is not supported for external Iceberg tables");
  }

  private static Distribution getIcebergDefaultDistribution(
      boolean isSorted, boolean isPartitioned) {
    if (isSorted) {
      return Distributions.RANGE;
    } else if (isPartitioned) {
      return Distributions.HASH;
    }
    return Distributions.NONE;
  }
}
