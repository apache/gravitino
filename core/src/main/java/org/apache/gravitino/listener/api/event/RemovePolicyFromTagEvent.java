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
package org.apache.gravitino.listener.api.event;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Represents an event triggered after successfully removing a policy from a tag. */
@DeveloperApi
public final class RemovePolicyFromTagEvent extends TagEvent {
  private final String metalake;
  private final String tagName;
  private final String policyName;

  /**
   * Constructs an event triggered after successfully removing a policy from a tag.
   *
   * <p>A successful event means the idempotent remove operation completed. It does not indicate
   * whether an association existed before the operation.
   *
   * @param user The user who initiated the operation.
   * @param metalake The metalake containing the tag and policy.
   * @param tagName The tag name.
   * @param policyName The policy name.
   */
  public RemovePolicyFromTagEvent(String user, String metalake, String tagName, String policyName) {
    super(user, NameIdentifierUtil.ofTag(metalake, tagName));
    this.metalake = metalake;
    this.tagName = tagName;
    this.policyName = policyName;
  }

  /**
   * @return The metalake containing the tag and policy.
   */
  public String metalake() {
    return metalake;
  }

  /**
   * @return The tag name.
   */
  public String tagName() {
    return tagName;
  }

  /**
   * @return The policy name.
   */
  public String policyName() {
    return policyName;
  }

  /** {@inheritDoc} */
  @Override
  protected Map<String, String> ownCustomInfo() {
    return ImmutableMap.of("policyName", policyName);
  }

  @Override
  public OperationType operationType() {
    return OperationType.REMOVE_POLICY_FROM_TAG;
  }
}
