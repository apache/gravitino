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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
 * Effective Gravitino / IRC auth agreement after cluster overlay.
 *
 * <p>Commit-chain and cron/snapshot-expiry merge both call {@link #hasSameAuth}. A {@code false}
 * means callers must not co-schedule or merge; they decide whether to skip or log.
 */
public final class MaintenanceAuthConsistency {

  private MaintenanceAuthConsistency() {}

  /**
   * Whether every policy in the list agrees on effective gravitino/IRC auth after cluster overlay.
   *
   * @param gravitinoConfig server config used for auth overlay
   * @param policies policies that would share one maintenance job (size &lt;= 1 always agrees)
   * @return true when all members share the same effective auth
   */
  public static boolean hasSameAuth(Config gravitinoConfig, List<PolicyEntity> policies) {
    Preconditions.checkNotNull(gravitinoConfig, "gravitinoConfig");
    Preconditions.checkNotNull(policies, "policies");
    if (policies.size() <= 1) {
      return true;
    }
    PolicyEntity first = policies.get(0);
    for (int i = 1; i < policies.size(); i++) {
      if (!hasSameAuth(gravitinoConfig, first, policies.get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Whether two policies agree on effective gravitino/IRC auth after cluster overlay.
   *
   * @param gravitinoConfig server config used for auth overlay
   * @param left policy
   * @param right policy
   * @return true when effective gravitino/IRC auth maps match
   */
  public static boolean hasSameAuth(Config gravitinoConfig, PolicyEntity left, PolicyEntity right) {
    Preconditions.checkNotNull(gravitinoConfig, "gravitinoConfig");
    Preconditions.checkNotNull(left, "left");
    Preconditions.checkNotNull(right, "right");
    Set<String> clusterIrcKeys = MaintenanceAuthConfigs.loadIrcAuth(gravitinoConfig).keySet();
    PolicyExecutionSpec leftSpec = executionSpec(left);
    PolicyExecutionSpec rightSpec = executionSpec(right);
    Map<String, String> leftGravitino =
        gravitinoAuthKeys(
            MaintenanceAuthConfigs.overlayUpdaterOptions(
                gravitinoConfig, leftSpec.updaterOptions()));
    Map<String, String> rightGravitino =
        gravitinoAuthKeys(
            MaintenanceAuthConfigs.overlayUpdaterOptions(
                gravitinoConfig, rightSpec.updaterOptions()));
    Map<String, String> leftIrc =
        ircAuthKeys(
            MaintenanceAuthConfigs.overlayJobOptions(gravitinoConfig, leftSpec.jobOptions()),
            clusterIrcKeys);
    Map<String, String> rightIrc =
        ircAuthKeys(
            MaintenanceAuthConfigs.overlayJobOptions(gravitinoConfig, rightSpec.jobOptions()),
            clusterIrcKeys);
    return leftGravitino.equals(rightGravitino) && leftIrc.equals(rightIrc);
  }

  /**
   * Whether two schedule snapshots agree on effective gravitino/IRC auth (same projection as the
   * policy overloads).
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
    Map<String, String> leftGravitino =
        gravitinoAuthKeys(
            IcebergSparkConfigUtils.parseFlatJsonMap(
                left.getUpdaterOptions(), MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY));
    Map<String, String> rightGravitino =
        gravitinoAuthKeys(
            IcebergSparkConfigUtils.parseFlatJsonMap(
                right.getUpdaterOptions(), MaintenanceJobTemplates.UPDATER_OPTIONS_JOB_CONF_KEY));
    Map<String, String> leftIrc =
        ircAuthKeys(
            IcebergSparkConfigUtils.parseFlatJsonMap(left.getSparkConf(), "spark_conf"),
            clusterIrcKeys);
    Map<String, String> rightIrc =
        ircAuthKeys(
            IcebergSparkConfigUtils.parseFlatJsonMap(right.getSparkConf(), "spark_conf"),
            clusterIrcKeys);
    return leftGravitino.equals(rightGravitino) && leftIrc.equals(rightIrc);
  }

  private static PolicyExecutionSpec executionSpec(PolicyEntity policy) {
    PolicyContent content = policy.content();
    return content instanceof ExecutablePolicyContent
        ? ((ExecutablePolicyContent) content).executionSpec()
        : PolicyExecutionSpec.empty();
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
