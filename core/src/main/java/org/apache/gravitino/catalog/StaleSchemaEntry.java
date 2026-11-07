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

/** An immutable entry in a stale report describing one stale schema registration. */
public class StaleSchemaEntry {

  private final String name;

  private final long detectedAt;

  private final boolean removed;

  /**
   * Creates a stale schema entry.
   *
   * @param name the schema name of the stale registration, in its logical form
   * @param detectedAt the epoch millisecond at which absence from the source was confirmed
   * @param removed whether the registration has been removed by the reconcile that detected it
   */
  public StaleSchemaEntry(String name, long detectedAt, boolean removed) {
    this.name = name;
    this.detectedAt = detectedAt;
    this.removed = removed;
  }

  /**
   * Returns the schema name of the stale registration, in its logical form (hierarchical names use
   * the external separator, e.g. {@code A:B}).
   *
   * @return the schema name
   */
  public String name() {
    return name;
  }

  /**
   * Returns the epoch millisecond at which absence from the source was confirmed.
   *
   * @return the detection timestamp in epoch milliseconds
   */
  public long detectedAt() {
    return detectedAt;
  }

  /**
   * Returns whether the registration has been removed by the reconcile that detected it. Always
   * {@code false} under the report-only policy; removal is applied by the auto-remove policy.
   *
   * @return whether the registration was removed
   */
  public boolean removed() {
    return removed;
  }
}
