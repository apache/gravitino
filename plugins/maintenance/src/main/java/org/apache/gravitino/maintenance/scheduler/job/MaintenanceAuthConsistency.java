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
package org.apache.gravitino.maintenance.scheduler.job;

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
import org.apache.gravitino.maintenance.optimizer.common.util.IcebergSparkConfigUtils;
import org.apache.gravitino.maintenance.policy.MaintenanceAuthConfigs;
import org.apache.gravitino.maintenance.scheduler.MaintenanceJobSpec;
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

    Set<String> clusterIrcKeys = MaintenanceAuthConfigs.loadIrcAuth(gravitinoConfig).keySet();
    Map<String, String> expectedGravitino = null;
    Map<String, String> expectedIrc = null;
    long expectedId = -1L;

    for (PolicyEntity policy : policies) {
      PolicyContent content = policy.content();
      PolicyExecutionSpec spec =
          content instanceof ExecutablePolicyContent
              ? ((ExecutablePolicyContent) content).executionSpec()
              : PolicyExecutionSpec.empty();
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
        expectedId = policy.id();
        continue;
      }
      if (!expectedGravitino.equals(gravitinoAuth)) {
        throw new IllegalArgumentException(
            mismatchMessage(
                "gravitinoAuth", expectedId, policy.id(), expectedGravitino, gravitinoAuth));
      }
      if (!Objects.equals(expectedIrc, ircAuth)) {
        throw new IllegalArgumentException(
            mismatchMessage("ircAuth", expectedId, policy.id(), expectedIrc, ircAuth));
      }
    }
  }

  /**
   * Compares enqueue snapshots. Spec maps are already cluster-overlaid at build time.
   *
   * @param gravitinoConfig server config (for cluster IRC key set)
   * @param left enqueue snapshot
   * @param right enqueue snapshot
   * @return true when both snapshots agree on effective gravitino/IRC auth
   */
  public static boolean agrees(
      Config gravitinoConfig, MaintenanceJobSpec left, MaintenanceJobSpec right) {
    Preconditions.checkNotNull(gravitinoConfig, "gravitinoConfig");
    Preconditions.checkNotNull(left, "left");
    Preconditions.checkNotNull(right, "right");
    Set<String> clusterIrcKeys = MaintenanceAuthConfigs.loadIrcAuth(gravitinoConfig).keySet();
    Map<String, String> leftUpdater =
        IcebergSparkConfigUtils.parseFlatJsonMap(
            left.getUpdaterOptions(), MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY);
    Map<String, String> rightUpdater =
        IcebergSparkConfigUtils.parseFlatJsonMap(
            right.getUpdaterOptions(), MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY);
    Map<String, String> leftSpark =
        IcebergSparkConfigUtils.parseFlatJsonMap(left.getSparkConf(), "spark_conf");
    Map<String, String> rightSpark =
        IcebergSparkConfigUtils.parseFlatJsonMap(right.getSparkConf(), "spark_conf");
    return projectGravitinoAuth(leftUpdater).equals(projectGravitinoAuth(rightUpdater))
        && projectIrcAuth(leftSpark, clusterIrcKeys)
            .equals(projectIrcAuth(rightSpark, clusterIrcKeys));
  }

  /**
   * Ids and differing key names only. Auth values (passwords / tokens) must not land in server
   * logs.
   */
  private static String mismatchMessage(
      String authName,
      long expectedId,
      long actualId,
      Map<String, String> expected,
      Map<String, String> actual) {
    Set<String> names = new TreeSet<>();
    names.addAll(expected.keySet());
    names.addAll(actual.keySet());
    List<String> differing = new ArrayList<>();
    for (String key : names) {
      if (!Objects.equals(expected.get(key), actual.get(key))) {
        differing.add(key);
      }
    }
    return String.format(
        "%s mismatch: policy %s vs policy %s; differing keys=%s",
        authName, expectedId, actualId, differing);
  }

  private static Map<String, String> projectGravitinoAuth(
      @Nullable Map<String, String> updaterOptions) {
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
  private static Map<String, String> projectIrcAuth(
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

  private static boolean isIrcAuthShaped(String key) {
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
}
