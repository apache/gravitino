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

package org.apache.gravitino.maintenance.optimizer.updater;

/** Supported updater actions. */
public enum UpdateType {
  /** Persist metrics derived from statistics. */
  METRICS,
  /** Persist statistics directly into the catalog. */
  STATISTICS;

  /**
   * Parses a case-insensitive update type value.
   *
   * @param value string representation such as "statistics" or "metrics"
   * @return matching {@link UpdateType}
   */
  public static UpdateType fromString(String value) {
    for (UpdateType m : UpdateType.values()) {
      if (m.name().equalsIgnoreCase(value)) {
        return m;
      }
    }
    throw new IllegalArgumentException(
        "Invalid update type: " + value + ". Allowed values: statistics, metrics");
  }
}
