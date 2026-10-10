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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Resolves effective {@code minIntervalMs} for a maintenance task type. */
public final class MinIntervalMsResolver {

  /** Prefix for per-type server configuration keys. */
  public static final String CONF_PREFIX = "gravitino.maintenance.task.";

  /** Suffix for minimum interval configuration keys. */
  public static final String CONF_SUFFIX = ".minIntervalMs";

  private MinIntervalMsResolver() {}

  /**
   * Returns the configuration key for a task type.
   *
   * @param taskType maintenance task type
   * @return full configuration key
   */
  public static String configKey(TableMaintenanceTaskType taskType) {
    Preconditions.checkNotNull(taskType, "taskType cannot be null");
    return CONF_PREFIX + taskType.configKey() + CONF_SUFFIX;
  }

  /**
   * Resolves minimum interval: policy content, then server configuration, then code default.
   *
   * @param taskType maintenance task type
   * @param policyMinIntervalMs value from effective policy content, or null
   * @param serverConfig gravitino server configuration map
   * @return resolved interval in milliseconds
   */
  public static long resolve(
      TableMaintenanceTaskType taskType,
      @Nullable Long policyMinIntervalMs,
      Map<String, String> serverConfig) {
    Preconditions.checkNotNull(taskType, "taskType cannot be null");
    Preconditions.checkNotNull(serverConfig, "serverConfig cannot be null");
    if (policyMinIntervalMs != null) {
      return policyMinIntervalMs;
    }
    String key = configKey(taskType);
    String configured = serverConfig.get(key);
    if (StringUtils.isNotBlank(configured)) {
      return Long.parseLong(configured.trim());
    }
    return taskType.defaultMinIntervalMs();
  }
}
