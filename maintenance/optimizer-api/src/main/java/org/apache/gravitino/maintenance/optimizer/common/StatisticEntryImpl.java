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

package org.apache.gravitino.maintenance.optimizer.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.gravitino.maintenance.optimizer.api.common.StatisticEntry;
import org.apache.gravitino.stats.StatisticValue;

/**
 * Immutable {@link StatisticEntry} implementation backed by Lombok.
 *
 * @param <T> underlying value type
 */
@Accessors(fluent = true)
@AllArgsConstructor
@EqualsAndHashCode
@Getter
public final class StatisticEntryImpl<T> implements StatisticEntry<T> {
  private final String name;
  private final StatisticValue<T> value;

  /**
   * Returns a concise string for logging and diagnostics.
   *
   * @return formatted string containing the statistic name and value
   */
  @Override
  public String toString() {
    return "{ " + name + " : " + value.value() + '}';
  }
}
