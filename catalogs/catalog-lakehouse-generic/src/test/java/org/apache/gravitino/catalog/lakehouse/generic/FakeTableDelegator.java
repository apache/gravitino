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
package org.apache.gravitino.catalog.lakehouse.generic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.ManagedSchemaOperations;
import org.apache.gravitino.catalog.ManagedTableOperations;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.storage.IdGenerator;

/**
 * A {@link LakehouseTableDelegator} registered only in the test classpath, whose table operations
 * touch nothing but the entity store. It lets the tests drive the catalog level table operations,
 * and the {@link TableLocationProvider} callbacks hanging off them, without a real table format
 * behind them.
 */
public class FakeTableDelegator implements LakehouseTableDelegator {

  /** The table format handled by this delegator. */
  public static final String TABLE_FORMAT = "testing";

  // Set by a test to make createTable behave like a format that declines the location it was
  // given: Lance's EXIST_OK mode returns the table that already exists, at the location that table
  // already had. Static because the catalog builds its own table operations through ServiceLoader.
  private static volatile String locationToUseInstead;

  /**
   * Makes every subsequent creation store the given location instead of the one the catalog
   * provisioned, the way a format returning an already existing table does.
   *
   * @param location the location the created table will report, null to create normally
   */
  public static void useLocationInstead(String location) {
    locationToUseInstead = location;
  }

  /** Stops overriding the location of created tables. */
  public static void reset() {
    locationToUseInstead = null;
  }

  @Override
  public String tableFormat() {
    return TABLE_FORMAT;
  }

  @Override
  public List<PropertyEntry<?>> tablePropertyEntries() {
    return ImmutableList.of();
  }

  @Override
  public ManagedTableOperations createTableOps(
      EntityStore store, ManagedSchemaOperations schemaOps, IdGenerator idGenerator) {
    return new ManagedTableOperations() {
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
          Index[] indexes) {
        String override = locationToUseInstead;
        Map<String, String> storedProperties = properties;
        if (override != null) {
          storedProperties = Maps.newHashMap(properties);
          storedProperties.put(Table.PROPERTY_LOCATION, override);
        }

        return super.createTable(
            ident,
            columns,
            comment,
            storedProperties,
            partitions,
            distribution,
            sortOrders,
            indexes);
      }
    };
  }
}
