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
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;

/**
 * Statistic interface represents a statistic that can be associated with a metadata object. It can
 * be used to store various types of statistics, for example, table statistics, partition
 * statistics, fileset statistics, etc.
 */
@Evolving
public interface Statistic extends Auditable {

  /** The prefix for custom statistics. Custom statistics are user-defined statistics. */
  String CUSTOM_PREFIX = "custom-";

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
  Optional<StatisticValue<?>> value();

  /**
   * The statistic is predefined by Gravitino if the value is true. The statistic is defined by
   * users if the value is false. For example, the statistic "row_count" is a reserved statistic, A
   * custom statistic name must start with "custom." prefix to avoid name conflict with reserved
   * statistics. Because Gravitino may add more reserved statistics in the future.
   *
   * @return The type of the statistic.
   */
  boolean reserved();

  /**
   * Whether the statistic is modifiable.
   *
   * @return If the statistic is modifiable, return true, otherwise false.
   */
  boolean modifiable();
}
