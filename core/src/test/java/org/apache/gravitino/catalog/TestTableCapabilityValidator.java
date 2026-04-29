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

import java.util.EnumSet;
import java.util.Set;
import org.apache.gravitino.connector.TableCapability;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.utils.TableCapabilityValidator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableCapabilityValidator {

  @Test
  public void testValidateIndexSupportWithIndexes() {
    Set<TableCapability> noIndexCaps =
        EnumSet.of(
            TableCapability.SUPPORTS_PARTITIONING,
            TableCapability.SUPPORTS_DISTRIBUTION,
            TableCapability.SUPPORTS_SORT_ORDERS,
            TableCapability.REQUIRES_LOCATION);
    Index[] indexes =
        new Index[] {Indexes.of(Index.IndexType.PRIMARY_KEY, "pk", new String[][] {{"col1"}})};

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TableCapabilityValidator.validateIndexSupport(noIndexCaps, indexes));
  }

  @Test
  public void testValidateIndexSupportWithIndexesAllowed() {
    Set<TableCapability> indexCaps = EnumSet.of(TableCapability.SUPPORTS_INDEX);
    Index[] indexes =
        new Index[] {Indexes.of(Index.IndexType.PRIMARY_KEY, "pk", new String[][] {{"col1"}})};

    Assertions.assertDoesNotThrow(
        () -> TableCapabilityValidator.validateIndexSupport(indexCaps, indexes));
  }

  @Test
  public void testValidateIndexSupportWithChanges() {
    Set<TableCapability> noIndexCaps =
        EnumSet.of(
            TableCapability.SUPPORTS_PARTITIONING,
            TableCapability.SUPPORTS_DISTRIBUTION,
            TableCapability.SUPPORTS_SORT_ORDERS,
            TableCapability.REQUIRES_LOCATION);
    TableChange[] changes =
        new TableChange[] {
          TableChange.addIndex(Index.IndexType.PRIMARY_KEY, "pk", new String[][] {{"col1"}})
        };

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TableCapabilityValidator.validateIndexSupport(noIndexCaps, changes));
  }

  @Test
  public void testValidateIndexSupportWithChangesAllowed() {
    Set<TableCapability> indexCaps = EnumSet.of(TableCapability.SUPPORTS_INDEX);
    TableChange[] changes =
        new TableChange[] {
          TableChange.addIndex(Index.IndexType.PRIMARY_KEY, "pk", new String[][] {{"col1"}})
        };

    Assertions.assertDoesNotThrow(
        () -> TableCapabilityValidator.validateIndexSupport(indexCaps, changes));
  }

  @Test
  public void testValidatePartitioningSupport() {
    Set<TableCapability> noPartitionCaps =
        EnumSet.of(
            TableCapability.SUPPORTS_INDEX,
            TableCapability.SUPPORTS_DISTRIBUTION,
            TableCapability.SUPPORTS_SORT_ORDERS,
            TableCapability.REQUIRES_LOCATION);
    Transform[] partitions = new Transform[] {Transforms.identity("col1")};

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            TableCapabilityValidator.validateTableLayoutSupport(
                noPartitionCaps, partitions, Distributions.NONE, new SortOrder[0]));
  }

  @Test
  public void testValidatePartitioningSupportAllowed() {
    Set<TableCapability> partitionCaps = EnumSet.of(TableCapability.SUPPORTS_PARTITIONING);
    Transform[] partitions = new Transform[] {Transforms.identity("col1")};

    Assertions.assertDoesNotThrow(
        () ->
            TableCapabilityValidator.validateTableLayoutSupport(
                partitionCaps, partitions, Distributions.NONE, new SortOrder[0]));
  }

  @Test
  public void testValidateDistributionSupport() {
    Set<TableCapability> noDistributionCaps =
        EnumSet.of(
            TableCapability.SUPPORTS_INDEX,
            TableCapability.SUPPORTS_PARTITIONING,
            TableCapability.SUPPORTS_SORT_ORDERS,
            TableCapability.REQUIRES_LOCATION);
    Distribution distribution = Distributions.of(Strategy.HASH, 4, NamedReference.field("col1"));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            TableCapabilityValidator.validateTableLayoutSupport(
                noDistributionCaps, new Transform[0], distribution, new SortOrder[0]));
  }

  @Test
  public void testValidateDistributionSupportAllowed() {
    Set<TableCapability> distributionCaps = EnumSet.of(TableCapability.SUPPORTS_DISTRIBUTION);
    Distribution distribution = Distributions.of(Strategy.HASH, 4, NamedReference.field("col1"));

    Assertions.assertDoesNotThrow(
        () ->
            TableCapabilityValidator.validateTableLayoutSupport(
                distributionCaps, new Transform[0], distribution, new SortOrder[0]));
  }

  @Test
  public void testValidateSortOrdersSupport() {
    Set<TableCapability> noSortOrdersCaps =
        EnumSet.of(
            TableCapability.SUPPORTS_INDEX,
            TableCapability.SUPPORTS_PARTITIONING,
            TableCapability.SUPPORTS_DISTRIBUTION,
            TableCapability.REQUIRES_LOCATION);
    SortOrder[] sortOrders = new SortOrder[] {SortOrders.ascending(NamedReference.field("col1"))};

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            TableCapabilityValidator.validateTableLayoutSupport(
                noSortOrdersCaps, new Transform[0], Distributions.NONE, sortOrders));
  }

  @Test
  public void testValidateSortOrdersSupportAllowed() {
    Set<TableCapability> sortOrderCaps = EnumSet.of(TableCapability.SUPPORTS_SORT_ORDERS);
    SortOrder[] sortOrders = new SortOrder[] {SortOrders.ascending(NamedReference.field("col1"))};

    Assertions.assertDoesNotThrow(
        () ->
            TableCapabilityValidator.validateTableLayoutSupport(
                sortOrderCaps, new Transform[0], Distributions.NONE, sortOrders));
  }
}
