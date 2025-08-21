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
package org.apache.gravitino.rel;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.stats.SupportsPartitionStatistics;
import org.apache.gravitino.stats.SupportsStatistics;
import org.apache.gravitino.tag.SupportsTags;

/**
 * An interface representing a table in a {@link Namespace}. It defines the basic properties of a
 * table. A catalog implementation with {@link TableCatalog} should implement this interface.
 */
@Evolving
public interface Table extends Auditable {

  /** @return Name of the table. */
  String name();

  /** @return The columns of the table. */
  Column[] columns();

  /** @return The physical partitioning of the table. */
  default Transform[] partitioning() {
    return Transforms.EMPTY_TRANSFORM;
  }

  /**
   * @return The sort order of the table. If no sort order is specified, an empty array is returned.
   */
  default SortOrder[] sortOrder() {
    return new SortOrder[0];
  }

  /**
   * @return The bucketing of the table. If no bucketing is specified, Distribution.NONE is
   *     returned.
   */
  default Distribution distribution() {
    return Distributions.NONE;
  }

  /**
   * @return The indexes of the table. If no indexes are specified, Indexes.EMPTY_INDEXES is
   *     returned.
   */
  default Index[] index() {
    return Indexes.EMPTY_INDEXES;
  }

  /** @return The comment of the table. Null is returned if no comment is set. */
  @Nullable
  default String comment() {
    return null;
  }

  /** @return The properties of the table. Empty map is returned if no properties are set. */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }

  /**
   * Table method for working with partitions. If the table does not support partition operations,
   * an {@link UnsupportedOperationException} is thrown.
   *
   * @return The partition support table.
   * @throws UnsupportedOperationException If the table does not support partition operations.
   */
  default SupportsPartitions supportPartitions() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Table does not support partition operations.");
  }

  /**
   * @return The {@link SupportsTags} if the table supports tag operations.
   * @throws UnsupportedOperationException If the table does not support tag operations.
   */
  default SupportsTags supportsTags() {
    throw new UnsupportedOperationException("Table does not support tag operations.");
  }

  /**
   * @return The {@link SupportsPolicies} if the table supports policy operations.
   * @throws UnsupportedOperationException If the table does not support policy operations.
   */
  default SupportsPolicies supportsPolicies() {
    throw new UnsupportedOperationException("Table does not support policy operations.");
  }

  /**
   * @return The {@link SupportsRoles} if the table supports role operations.
   * @throws UnsupportedOperationException If the table does not support role operations.
   */
  default SupportsRoles supportsRoles() {
    throw new UnsupportedOperationException("Table does not support role operations.");
  }

  /**
   * Returns the {@link SupportsStatistics} if the table supports statistics operations.
   *
   * @return The {@link SupportsStatistics} for the table.
   */
  default SupportsStatistics supportsStatistics() {
    throw new UnsupportedOperationException("Table does not support statistics operations.");
  }

  /**
   * Returns the {@link SupportsPartitionStatistics} if the table supports partition statistics
   *
   * @return The {@link SupportsPartitionStatistics} for the table.
   */
  default SupportsPartitionStatistics supportsPartitionStatistics() {
    throw new UnsupportedOperationException(
        "Table does not support partition statistics operations.");
  }
}
