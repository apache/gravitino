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
package org.apache.gravitino.stats;

import java.util.Optional;
import org.apache.gravitino.annotation.Evolving;

/**
 * Statistic interface represents a statistic that can be associated with a metadata object. It can
 * be used to store various types of statistics, for example, table statistics, partition
 * statistics, fileset statistics, etc.
 *
 * <p>For different statistics, Gravitino will use different storage solution for them. Table
 * statistics will be stored in Gravitino metadata store, usually the Gravitino's metadata is
 * relational database, it's difficult to store millions rows of records.
 *
 * <p>But for partition statistics, there may be millions of partitions in a large table. So
 * Gravitino will use a external storage solution. Gravitino will use a file to store all
 * partitions. The partition statistics may be several megabytes. If a partition is stored as a
 * file, it will cause millions of small files in the storage system. It will cause high pressure
 * for HDFS if you choose HDFS as external storage. If the partition statistic is requested for
 * multiple times, Gravitino will use cache to improve performance.
 *
 * <p>The file format of partition statistics is Json, Iceberg isn't design for small files. It will
 * read multiple files in a single read operation. It will append many small data files if users
 * update a partition every time. The relational database will record the pointer of the partition
 * statistics file.
 */
@Evolving
public interface Statistic {

  /**
   * Get the name of the statistic.
   *
   * @return The name of the statistic.
   */
  String name();

  /**
   * Get the value of the statistic. The value is optional. If the statistic is not set, the value
   * will be empty.
   *
   * @return An optional containing the value of the statistic if it is set, otherwise empty.
   */
  Optional<StatisticValue> value();

  /**
   * Get the type of the statistic. The type can be either RESERVED or CUSTOM. RESERVED means the
   * statistic is predefined by Gravitino, CUSTOM means the statistic is defined by users. For
   * example, the statistic "row_count" is a reserved statistic, A custom statistic name must start
   * with "custom." prefix to avoid name conflict with reserved statistics. Because Gravitino may
   * add more reserved statistics in the future.
   *
   * @return The type of the statistic.
   */
  Type type();

  /**
   * Whether the statistic is modifiable.
   *
   * @return If the statistic is modifiable, return true, otherwise false.
   */
  boolean modifiable();

  /** The type of the statistic. */
  enum Type {
    /** Gravitino predefined statistic type. */
    RESERVED,
    /** User custom statistic type. */
    CUSTOM
  };
}
