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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import java.util.Optional;

/**
 * Builds and parses db-scheduler instance ids for long-lived {@code policy-expand} rows (§5.2.1).
 *
 * <p>Format: {@code policy:{policy_id}}.
 */
public final class PolicyExpandIds {

  private static final String POLICY_PREFIX = "policy:";

  private PolicyExpandIds() {}

  /**
   * @param policyId policy entity id
   * @return policy-expand instance id {@code policy:{policy_id}}
   */
  public static String forPolicy(long policyId) {
    return POLICY_PREFIX + policyId;
  }

  /**
   * Parses a policy-expand instance id.
   *
   * @param instanceId db-scheduler task instance id
   * @return policy id when the id matches {@code policy:{policyId}}
   */
  public static Optional<Long> parsePolicy(String instanceId) {
    if (instanceId == null || !instanceId.startsWith(POLICY_PREFIX)) {
      return Optional.empty();
    }
    String rest = instanceId.substring(POLICY_PREFIX.length());
    if (rest.isEmpty() || rest.indexOf(':') >= 0) {
      return Optional.empty();
    }
    try {
      return Optional.of(Long.parseLong(rest));
    } catch (NumberFormatException e) {
      return Optional.empty();
    }
  }
}
