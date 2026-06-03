/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.utils;

import java.util.Arrays;
import java.util.Set;
import org.apache.gravitino.connector.TableCapability;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

public final class TableCapabilityValidator {

  private TableCapabilityValidator() {}

  public static void validateIndexSupport(Set<TableCapability> capabilities, Index[] indexes) {
    if (!capabilities.contains(TableCapability.SUPPORTS_INDEX)
        && indexes != null
        && indexes.length > 0) {
      throw new IllegalArgumentException("Indexes are not supported by this catalog");
    }
  }

  public static void validateIndexSupport(
      Set<TableCapability> capabilities, TableChange[] changes) {
    if (capabilities.contains(TableCapability.SUPPORTS_INDEX)) {
      return;
    }
    boolean hasIndexChange =
        Arrays.stream(changes)
            .anyMatch(
                change ->
                    change instanceof TableChange.AddIndex
                        || change instanceof TableChange.DeleteIndex);
    if (hasIndexChange) {
      throw new IllegalArgumentException("Indexes are not supported by this catalog");
    }
  }

  public static void validateTableLayoutSupport(
      Set<TableCapability> capabilities,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders) {
    if (!capabilities.contains(TableCapability.SUPPORTS_PARTITIONING)
        && partitions != null
        && partitions.length > 0) {
      throw new IllegalArgumentException("Partitioning is not supported by this catalog");
    }

    if (!capabilities.contains(TableCapability.SUPPORTS_DISTRIBUTION)
        && distribution != null
        && !Distributions.NONE.equals(distribution)) {
      throw new IllegalArgumentException("Distribution is not supported by this catalog");
    }

    if (!capabilities.contains(TableCapability.SUPPORTS_SORT_ORDERS)
        && sortOrders != null
        && sortOrders.length > 0) {
      throw new IllegalArgumentException("Sort orders are not supported by this catalog");
    }
  }
}
