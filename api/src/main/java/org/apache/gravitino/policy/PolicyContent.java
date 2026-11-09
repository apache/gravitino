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
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.gravitino.MetadataObject;

/**
 * The interface of the content of the policy.
 *
 * <p>Execution peers ({@code timeTrigger}, {@code commitTrigger}, {@code minIntervalMs}, {@code
 * jobOptions}, {@code updaterOptions}) are not part of this universal interface. Built-in Iceberg
 * maintenance contents carry them via {@link ExecutablePolicyContent}/{@link PolicyExecutionSpec}.
 */
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
   * Validates the policy content.
   *
   * <p>Checks universal fields such as {@link #supportedObjectTypes()}. Maintenance execution-field
   * validation lives on {@link ExecutablePolicyContent} implementations.
   *
   * @throws IllegalArgumentException if the content is invalid.
   */
  default void validate() throws IllegalArgumentException {
    Preconditions.checkArgument(
        CollectionUtils.isNotEmpty(supportedObjectTypes()), "supportedObjectTypes cannot be empty");
  }

  /**
   * Validates that this content may become effective on a metadata object reached through a tag.
   *
   * <p>Invoked on write paths that associate a policy with a tag, assign a tagged policy to an
   * object, or update content while tag placements already exist. Default is a no-op; built-in
   * maintenance contents override when they have object-scoped parameter constraints (for example
   * orphan-scan {@code location} containment).
   *
   * @param objectType type of the metadata object
   * @param objectProperties object properties available for the check (for tables, typically
   *     including {@code location}); may be null or empty when the object has none or is not loaded
   * @throws IllegalArgumentException if the content parameters are incompatible with the object
   */
  default void requireCompatibleBinding(
      MetadataObject.Type objectType, @Nullable Map<String, String> objectProperties) {
    // No object-scoped constraints by default.
  }
}
