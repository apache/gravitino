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
package org.apache.gravitino.maintenance.scheduler.job.commit;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.gravitino.Config;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyExecutionSpec;

/**
 * Fail-closed checks that commit-chain members agree on effective Gravitino / IRC auth after
 * cluster overlay.
 *
 * <p>Cluster-static {@code gravitinoAuth} / {@code ircAuth} normally make every policy identical.
 * When conf leaves those overlays empty, policies may carry different auth-shaped options; a single
 * chained Spark Job cannot pick one identity silently.
 */
final class CommitAuthConsistency {

  private CommitAuthConsistency() {}

  /**
   * @param chain filtered commit-chain members (same table)
   * @param gravitinoConfig server config used for auth overlay
   * @throws IllegalArgumentException when effective auth options disagree
   */
  static void requireConsistent(List<CommitChainMember> chain, Config gravitinoConfig) {
    Preconditions.checkNotNull(chain);
    Preconditions.checkNotNull(gravitinoConfig);
    if (chain.size() <= 1) {
      return;
    }

    Set<String> clusterIrcKeys = MaintenanceAuthConfigs.loadIrcAuth(gravitinoConfig).keySet();
    Map<String, String> expectedGravitino = null;
    Map<String, String> expectedIrc = null;
    long expectedPolicyId = -1L;

    for (CommitChainMember member : chain) {
      PolicyEntity policy = member.policy();
      PolicyExecutionSpec spec = executionSpec(policy.content());
      Map<String, String> gravitinoAuth =
          projectGravitinoAuth(
              MaintenanceAuthConfigs.overlayUpdaterOptions(gravitinoConfig, spec.updaterOptions()));
      Map<String, String> ircAuth =
          projectIrcAuth(
              MaintenanceAuthConfigs.overlayJobOptions(gravitinoConfig, spec.jobOptions()),
              clusterIrcKeys);

      if (expectedGravitino == null) {
        expectedGravitino = gravitinoAuth;
        expectedIrc = ircAuth;
        expectedPolicyId = policy.id();
        continue;
      }
      if (!expectedGravitino.equals(gravitinoAuth)) {
        throw new IllegalArgumentException(
            mismatchMessage(
                "gravitinoAuth", expectedPolicyId, policy.id(), expectedGravitino, gravitinoAuth));
      }
      if (!Objects.equals(expectedIrc, ircAuth)) {
        throw new IllegalArgumentException(
            mismatchMessage("ircAuth", expectedPolicyId, policy.id(), expectedIrc, ircAuth));
      }
    }
  }

  /**
   * Builds a mismatch message with policy ids and differing key names only — never auth values
   * (passwords / tokens must not land in server logs).
   */
  static String mismatchMessage(
      String bagName,
      long expectedPolicyId,
      long actualPolicyId,
      Map<String, String> expected,
      Map<String, String> actual) {
    return String.format(
        "commit-chain %s mismatch: policy %s vs policy %s; differing keys=%s",
        bagName, expectedPolicyId, actualPolicyId, differingKeys(expected, actual));
  }

  /** Key names that are missing, extra, or present with unequal values. */
  static List<String> differingKeys(Map<String, String> left, Map<String, String> right) {
    Set<String> names = new TreeSet<>();
    names.addAll(left.keySet());
    names.addAll(right.keySet());
    List<String> differing = new ArrayList<>();
    for (String key : names) {
      if (!Objects.equals(left.get(key), right.get(key))) {
        differing.add(key);
      }
    }
    return differing;
  }

  static Map<String, String> projectGravitinoAuth(@Nullable Map<String, String> updaterOptions) {
    Map<String, String> projected = new TreeMap<>();
    if (updaterOptions == null) {
      return projected;
    }
    for (String key : MaintenanceAuthConfigs.GRAVITINO_AUTH_UPDATER_OPTION_KEYS) {
      String value = updaterOptions.get(key);
      if (value != null) {
        projected.put(key, value);
      }
    }
    return projected;
  }

  /**
   * Projects IRC auth-shaped keys: every cluster {@code ircAuth} key present after overlay, plus
   * any remaining jobOption key that looks like catalog auth (so policy-only auth still conflicts).
   */
  static Map<String, String> projectIrcAuth(
      @Nullable Map<String, String> overlaidJobOptions, Set<String> clusterIrcKeys) {
    Map<String, String> projected = new TreeMap<>();
    if (overlaidJobOptions == null) {
      return projected;
    }
    for (Map.Entry<String, String> entry : overlaidJobOptions.entrySet()) {
      String key = entry.getKey();
      if (key == null || entry.getValue() == null) {
        continue;
      }
      if (clusterIrcKeys.contains(key) || isIrcAuthShaped(key)) {
        projected.put(key, entry.getValue());
      }
    }
    return projected;
  }

  static boolean isIrcAuthShaped(String key) {
    String lower = key.toLowerCase(Locale.ROOT);
    return lower.contains(".auth.")
        || lower.startsWith("auth.")
        || lower.startsWith("rest.auth")
        || lower.contains("oauth")
        || lower.contains("keytab")
        || lower.contains("credential")
        || lower.endsWith(".token")
        || lower.endsWith(".password");
  }

  private static PolicyExecutionSpec executionSpec(@Nullable PolicyContent content) {
    if (content instanceof ExecutablePolicyContent) {
      return ((ExecutablePolicyContent) content).executionSpec();
    }
    return PolicyExecutionSpec.empty();
  }
}
