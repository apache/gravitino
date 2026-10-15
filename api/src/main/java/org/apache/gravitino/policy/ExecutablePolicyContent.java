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
package org.apache.gravitino.policy;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Policy content that carries immutable execution peers for automated maintenance.
 *
 * <p>Row-filter, column-mask, and custom contents are not executable in this sense. Callers that
 * need peers should use {@code instanceof ExecutablePolicyContent} (or a concrete maintenance
 * content type) rather than assuming every {@link PolicyContent} exposes them.
 */
public interface ExecutablePolicyContent extends PolicyContent {

  /**
   * @return immutable execution peers; never {@code null}
   */
  PolicyExecutionSpec executionSpec();

  /**
   * @return cron expression, or {@code null} when unset
   */
  @Nullable
  default String timeTrigger() {
    return executionSpec().timeTrigger();
  }

  /**
   * @return commit-trigger flag when explicitly supplied (including {@code false}), or {@code null}
   *     when unset
   */
  @Nullable
  default Boolean commitTrigger() {
    return executionSpec().commitTrigger();
  }

  /**
   * @return minimum interval in milliseconds, or {@code null} when unset
   */
  @Nullable
  default Long minIntervalMs() {
    return executionSpec().minIntervalMs();
  }

  /**
   * @return job options; never {@code null}
   */
  default Map<String, String> jobOptions() {
    return executionSpec().jobOptions();
  }

  /**
   * @return updater options; never {@code null}
   */
  default Map<String, String> updaterOptions() {
    return executionSpec().updaterOptions();
  }
}
