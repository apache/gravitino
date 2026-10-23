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

import java.util.Locale;

/**
 * The policy a reconcile run applies to detected stale registrations.
 *
 * <p>The policy is global-only in the first version; per-catalog overrides are a possible future
 * extension.
 */
public enum ReconcilePolicy {

  /** Stale registrations are reported only; nothing is removed. This is the default. */
  REPORT_ONLY,

  /** Stale registrations are removed through the dispatcher chain, like an explicit drop. */
  AUTO_REMOVE;

  /**
   * Resolves a policy from its configuration value. The value is case-insensitive and accepts both
   * the hyphenated form (e.g. {@code report-only}) used in configuration files and the underscored
   * enum name.
   *
   * @param value the configuration value, for example {@code report-only} or {@code auto-remove}
   * @return the resolved policy, or the default {@link #REPORT_ONLY} if the value is null or not a
   *     valid policy name
   */
  public static ReconcilePolicy fromString(String value) {
    if (value == null) {
      return REPORT_ONLY;
    }
    String normalized = value.trim().replace('-', '_');
    for (ReconcilePolicy policy : values()) {
      if (policy.name().equalsIgnoreCase(normalized)) {
        return policy;
      }
    }
    return REPORT_ONLY;
  }

  /**
   * Returns the canonical configuration value for this policy, e.g. {@code report-only}.
   *
   * @return the hyphenated lowercase form used in configuration files
   */
  public String configValue() {
    return name().toLowerCase(Locale.ROOT).replace('_', '-');
  }
}
