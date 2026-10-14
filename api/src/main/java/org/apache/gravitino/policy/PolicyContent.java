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

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.gravitino.MetadataObject;

/** The interface of the content of the policy. */
public interface PolicyContent {

  /**
   * @return the set of metadata object types that the policy can be applied to
   */
  Set<MetadataObject.Type> supportedObjectTypes();

  /**
   * @return The additional properties of the policy.
   */
  Map<String, String> properties();

  /**
   * A convenience method to get all rules in the policy content.
   *
   * @return A map of rule names to their corresponding rule objects.
   */
  default Map<String, Object> rules() {
    // backward compatibility
    throw new UnsupportedOperationException("Does support get all rules.");
  }

  /**
   * Optional time-based trigger for automated policy evaluation.
   *
   * @return time trigger, or {@code null} when unset
   */
  @Nullable
  default TimeTrigger timeTrigger() {
    return null;
  }

  /**
   * Optional commit trigger for automated policy evaluation after an Iceberg commit.
   *
   * @return commit trigger, or {@code null} when unset
   */
  @Nullable
  default CommitTrigger commitTrigger() {
    return null;
  }

  /**
   * Optional cooldown between finished automated runs for this policy.
   *
   * @return minimum interval in milliseconds, or {@code null} when unset
   */
  @Nullable
  default Long minIntervalMs() {
    return null;
  }

  /**
   * Optional job / client options carried in policy content (non-secret).
   *
   * @return job options; never {@code null}
   */
  default Map<String, String> jobOptions() {
    return Collections.emptyMap();
  }

  /**
   * Optional updater / control-plane options carried in policy content (non-secret).
   *
   * @return update options; never {@code null}
   */
  default Map<String, String> updateOptions() {
    return Collections.emptyMap();
  }

  /**
   * Validates the policy content.
   *
   * @throws IllegalArgumentException if the content is invalid.
   */
  default void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        CollectionUtils.isNotEmpty(supportedObjectTypes()), "supportedObjectTypes cannot be empty");
  }
}
