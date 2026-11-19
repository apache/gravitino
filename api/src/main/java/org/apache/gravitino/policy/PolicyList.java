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
import org.apache.gravitino.annotation.Evolving;

/**
 * A list of visible policies and whether authorization filtering omitted other policies.
 *
 * @param <T> The type of the returned policy names or policy objects.
 */
@Evolving
public final class PolicyList<T> {

  private final T[] policies;
  private final boolean hasInaccessiblePolicies;

  /**
   * Creates a policy list.
   *
   * @param policies The visible policy names or policy objects.
   * @param hasInaccessiblePolicies Whether one or more inaccessible policies were omitted.
   */
  public PolicyList(T[] policies, boolean hasInaccessiblePolicies) {
    Preconditions.checkArgument(policies != null, "Policies must not be null");
    this.policies = policies.clone();
    this.hasInaccessiblePolicies = hasInaccessiblePolicies;
  }

  /**
   * @return The visible policy names or policy objects.
   */
  public T[] policies() {
    return policies.clone();
  }

  /**
   * @return True if one or more inaccessible policies were omitted, false otherwise.
   */
  public boolean hasInaccessiblePolicies() {
    return hasInaccessiblePolicies;
  }
}
