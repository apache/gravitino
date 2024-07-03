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
package com.datastrato.gravitino.rel;

import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.EMPTY_TRANSFORM;

import com.datastrato.gravitino.Auditable;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

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
    return EMPTY_TRANSFORM;
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
}
