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
package org.apache.gravitino.maintenance.policy;

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
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ExecutablePolicyContent;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyExecutionSpec;

/**
 * Fail-closed checks that merged maintenance work agrees on effective Gravitino / IRC auth after
 * cluster overlay.
 *
 * <p>Used by commit-chain enqueue (one Spark job, many policies) and snapshot-expiry coalesce
 * (shared {@code updater_options} / {@code spark_conf} on the survivor row). Cluster-static {@code
 * gravitinoAuth} / {@code ircAuth} normally make every policy identical; when overlays are empty,
 * disagreeing policy auth must not be merged silently.
 */
public final class MaintenanceAuthConsistency {

  private MaintenanceAuthConsistency() {}

  /**
   * Effective auth view for one policy or enqueue snapshot (id is for error messages only).
   *
   * <p>{@code updaterOptions} / {@code jobOptions} should already include cluster overlay when
   * built from enqueue {@code task_data}; {@link #fromPolicy} applies overlay itself.
   */
  public static final class AuthView {
    private final long id;
    private final Map<String, String> updaterOptions;
    private final Map<String, String> jobOptions;

    /**
     * @param id policy or row id for mismatch messages
     * @param updaterOptions overlaid updater options (may be empty)
     * @param jobOptions overlaid job options or spark_conf-derived auth-shaped map (may be empty)
     */
    public AuthView(
        long id,
        @Nullable Map<String, String> updaterOptions,
        @Nullable Map<String, String> jobOptions) {
      this.id = id;
      this.updaterOptions = updaterOptions == null ? Map.of() : updaterOptions;
      this.jobOptions = jobOptions == null ? Map.of() : jobOptions;
    }

    /**
     * @return id used in mismatch messages
     */
    public long id() {
      return id;
    }
  }

  /**
   * Builds an auth view from a policy by applying cluster gravitino/IRC overlays.
   *
   * @param policy policy entity
   * @param gravitinoConfig server config
   * @return overlaid auth view
   */
  public static AuthView fromPolicy(PolicyEntity policy, Config gravitinoConfig) {
    Preconditions.checkNotNull(policy, "policy");
    Preconditions.checkNotNull(gravitinoConfig, "gravitinoConfig");
    PolicyExecutionSpec spec = executionSpec(policy.content());
    return new AuthView(
        policy.id(),
        MaintenanceAuthConfigs.overlayUpdaterOptions(gravitinoConfig, spec.updaterOptions()),
        MaintenanceAuthConfigs.overlayJobOptions(gravitinoConfig, spec.jobOptions()));
  }

  /**
   * @param policies commit-chain (or other) policy members
   * @param gravitinoConfig server config used for auth overlay
   * @throws IllegalArgumentException when effective auth options disagree
   */
  public static void requirePoliciesConsistent(
      List<PolicyEntity> policies, Config gravitinoConfig) {
    Preconditions.checkNotNull(policies, "policies");
    Preconditions.checkNotNull(gravitinoConfig, "gravitinoConfig");
    if (policies.size() <= 1) {
      return;
    }
    List<AuthView> views = new ArrayList<>(policies.size());
    for (PolicyEntity policy : policies) {
      views.add(fromPolicy(policy, gravitinoConfig));
    }
    requireConsistent(gravitinoConfig, views);
  }

  /**
   * Compares already-overlaid auth views (for example enqueue {@code task_data} snapshots).
   *
   * @param gravitinoConfig server config (for cluster IRC key set)
   * @param views auth views to compare; size &lt;= 1 is a no-op
   * @throws IllegalArgumentException when effective auth options disagree
   */
  public static void requireConsistent(Config gravitinoConfig, List<AuthView> views) {
    Preconditions.checkNotNull(gravitinoConfig, "gravitinoConfig");
    Preconditions.checkNotNull(views, "views");
    if (views.size() <= 1) {
      return;
    }

    Set<String> clusterIrcKeys = MaintenanceAuthConfigs.loadIrcAuth(gravitinoConfig).keySet();
    Map<String, String> expectedGravitino = null;
    Map<String, String> expectedIrc = null;
    long expectedId = -1L;

    for (AuthView view : views) {
      Map<String, String> gravitinoAuth = projectGravitinoAuth(view.updaterOptions);
      Map<String, String> ircAuth = projectIrcAuth(view.jobOptions, clusterIrcKeys);

      if (expectedGravitino == null) {
        expectedGravitino = gravitinoAuth;
        expectedIrc = ircAuth;
        expectedId = view.id;
        continue;
      }
      if (!expectedGravitino.equals(gravitinoAuth)) {
        throw new IllegalArgumentException(
            mismatchMessage(
                "gravitinoAuth", expectedId, view.id, expectedGravitino, gravitinoAuth));
      }
      if (!Objects.equals(expectedIrc, ircAuth)) {
        throw new IllegalArgumentException(
            mismatchMessage("ircAuth", expectedId, view.id, expectedIrc, ircAuth));
      }
    }
  }

  /**
   * @return true when both views agree on effective gravitino/IRC auth after projection
   */
  public static boolean agrees(Config gravitinoConfig, AuthView left, AuthView right) {
    try {
      requireConsistent(gravitinoConfig, List.of(left, right));
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Builds a mismatch message with ids and differing key names only — never auth values (passwords
   * / tokens must not land in server logs).
   */
  public static String mismatchMessage(
      String authName,
      long expectedId,
      long actualId,
      Map<String, String> expected,
      Map<String, String> actual) {
    return String.format(
        "%s mismatch: policy %s vs policy %s; differing keys=%s",
        authName, expectedId, actualId, differingKeys(expected, actual));
  }

  /** Key names that are missing, extra, or present with unequal values. */
  public static List<String> differingKeys(Map<String, String> left, Map<String, String> right) {
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
   * any remaining jobOption / spark_conf key that looks like catalog auth.
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
