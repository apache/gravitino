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

package org.apache.gravitino.listener.api.info;

import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContent;

/**
 * Provides read-only access to policy information for event listeners. This class encapsulates the
 * essential attributes of a Policy, including its name, type, comment, enabled status, content, and
 * audit information.
 */
@DeveloperApi
public final class PolicyInfo {
  private final String name;
  private final String policyType;
  @Nullable private final String comment;
  private final boolean enabled;
  private final PolicyContent content;
  @Nullable private final Audit audit;

  /**
   * Constructs PolicyInfo from a Policy object.
   *
   * @param policy The Policy object to extract information from.
   */
  public PolicyInfo(Policy policy) {
    this(
        policy.name(),
        policy.policyType(),
        policy.comment(),
        policy.enabled(),
        policy.content(),
        policy.auditInfo());
  }

  /**
   * Constructs PolicyInfo with detailed parameters.
   *
   * @param name The name of the policy.
   * @param policyType The type of the policy.
   * @param comment An optional description of the policy.
   * @param enabled Whether the policy is enabled.
   * @param content The content of the policy.
   * @param audit Optional audit information.
   */
  public PolicyInfo(
      String name,
      String policyType,
      String comment,
      boolean enabled,
      PolicyContent content,
      Audit audit) {
    this.name = name;
    this.policyType = policyType;
    this.comment = comment;
    this.enabled = enabled;
    this.content = content;
    this.audit = audit;
  }

  /**
   * Returns the name of the policy.
   *
   * @return The policy name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the type of the policy.
   *
   * @return The policy type.
   */
  public String policyType() {
    return policyType;
  }

  /**
   * Returns the optional comment describing the policy.
   *
   * @return The comment, or null if not provided.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * Returns whether the policy is enabled.
   *
   * @return True if the policy is enabled, false otherwise.
   */
  public boolean enabled() {
    return enabled;
  }

  /**
   * Returns the content of the policy.
   *
   * @return The policy content.
   */
  public PolicyContent content() {
    return content;
  }

  /**
   * Returns the optional audit information.
   *
   * @return The audit information, or null if not provided.
   */
  @Nullable
  public Audit audit() {
    return audit;
  }
}
