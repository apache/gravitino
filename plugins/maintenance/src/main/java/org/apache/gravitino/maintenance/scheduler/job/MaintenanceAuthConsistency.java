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
 * <p>Commit-chain schedule uses {@link #requireSameAuth}; snapshot-expiry merge uses {@link
 * #hasSameAuth}. Both project gravitino/IRC auth maps through the same helpers then compare.
 * Cluster-static {@code gravitinoAuth} / {@code ircAuth} normally make every policy identical; when
 * overlays are empty, disagreeing policy auth must not be merged silently.
 */
public final class MaintenanceAuthConsistency {

  private MaintenanceAuthConsistency() {}

  /**
   * Fail-closed check for commit-chain (or other) policy members.
   *
   * @param policies commit-chain (or other) policy members
   * @param gravitinoConfig server config used for auth overlay
   * @throws IllegalArgumentException when effective auth options disagree
   */
  public static void requireSameAuth(List<PolicyEntity> policies, Config gravitinoConfig) {
    Preconditions.checkNotNull(policies, "policies");
    Preconditions.checkNotNull(gravitinoConfig, "gravitinoConfig");
    if (policies.size() <= 1) {
      return;
    }

    Set<String> clusterIrcKeys = MaintenanceAuthConfigs.loadIrcAuth(gravitinoConfig).keySet();
    PolicyEntity first = policies.get(0);
    Map<String, String> expectedGravitino = gravitinoAuthFromPolicy(first, gravitinoConfig);
    Map<String, String> expectedIrc = ircAuthFromPolicy(first, gravitinoConfig, clusterIrcKeys);
    for (int i = 1; i < policies.size(); i++) {
      PolicyEntity policy = policies.get(i);
      Map<String, String> gravitino = gravitinoAuthFromPolicy(policy, gravitinoConfig);
      Map<String, String> irc = ircAuthFromPolicy(policy, gravitinoConfig, clusterIrcKeys);
      if (!expectedGravitino.equals(gravitino)) {
        throw new IllegalArgumentException(
            differingKeysMessage(
                "gravitinoAuth", first.id(), policy.id(), expectedGravitino, gravitino));
      }
      if (!expectedIrc.equals(irc)) {
        throw new IllegalArgumentException(
            differingKeysMessage("ircAuth", first.id(), policy.id(), expectedIrc, irc));
      }
    }
  }

  /**
   * Compares two schedule snapshots after projecting gravitino/IRC auth the same way as {@link
   * #requireSameAuth}.
   *
   * @param gravitinoConfig server config (for cluster IRC key set)
   * @param left schedule snapshot
   * @param right schedule snapshot
   * @return true when both snapshots agree on effective gravitino/IRC auth
   */
  public static boolean hasSameAuth(
      Config gravitinoConfig, MaintenanceJobSpec left, MaintenanceJobSpec right) {
    Preconditions.checkNotNull(gravitinoConfig, "gravitinoConfig");
    Preconditions.checkNotNull(left, "left");
    Preconditions.checkNotNull(right, "right");
    Set<String> clusterIrcKeys = MaintenanceAuthConfigs.loadIrcAuth(gravitinoConfig).keySet();
    return gravitinoAuthFromSpec(left).equals(gravitinoAuthFromSpec(right))
        && ircAuthFromSpec(left, clusterIrcKeys).equals(ircAuthFromSpec(right, clusterIrcKeys));
  }

  private static Map<String, String> gravitinoAuthFromPolicy(
      PolicyEntity policy, Config gravitinoConfig) {
    return gravitinoAuthKeys(
        MaintenanceAuthConfigs.overlayUpdaterOptions(
            gravitinoConfig, executionSpec(policy).updaterOptions()));
  }

  private static Map<String, String> ircAuthFromPolicy(
      PolicyEntity policy, Config gravitinoConfig, Set<String> clusterIrcKeys) {
    return ircAuthKeys(
        MaintenanceAuthConfigs.overlayJobOptions(
            gravitinoConfig, executionSpec(policy).jobOptions()),
        clusterIrcKeys);
  }

  private static Map<String, String> gravitinoAuthFromSpec(MaintenanceJobSpec spec) {
    return gravitinoAuthKeys(
        IcebergSparkConfigUtils.parseFlatJsonMap(
            spec.getUpdaterOptions(), MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY));
  }

  private static Map<String, String> ircAuthFromSpec(
      MaintenanceJobSpec spec, Set<String> clusterIrcKeys) {
    return ircAuthKeys(
        IcebergSparkConfigUtils.parseFlatJsonMap(spec.getSparkConf(), "spark_conf"),
        clusterIrcKeys);
  }

  private static PolicyExecutionSpec executionSpec(PolicyEntity policy) {
    PolicyContent content = policy.content();
    return content instanceof ExecutablePolicyContent
        ? ((ExecutablePolicyContent) content).executionSpec()
        : PolicyExecutionSpec.empty();
  }

  /**
   * Ids and differing key names only. Auth values (passwords / tokens) must not land in server
   * logs.
   */
  private static String differingKeysMessage(
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

  private static Map<String, String> gravitinoAuthKeys(
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

  /** Keeps cluster {@code ircAuth} keys and any other key that looks like catalog auth. */
  private static Map<String, String> ircAuthKeys(
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
      if (clusterIrcKeys.contains(key) || isCatalogAuthKey(key)) {
        projected.put(key, entry.getValue());
      }
    }
    return projected;
  }

  private static boolean isCatalogAuthKey(String key) {
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
