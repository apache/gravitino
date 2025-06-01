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

import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;

/**
 * The interface for supporting getting or associating policies with a metadata object. This
 * interface will be mixed with metadata objects to provide policy operations.
 */
@Evolving
public interface SupportsPolicies {
  /** @return List all the policy names for the specific object. */
  String[] listPolicies();

  /** @return List all the policies with details for the specific object. */
  Policy[] listPolicyInfos();

  /**
   * Get a policy by its name for the specific object.
   *
   * @param name The name of the policy.
   * @return The policy.
   * @throws NoSuchPolicyException If the policy does not associate with the object.
   */
  Policy getPolicy(String name) throws NoSuchPolicyException;

  /**
   * Associate policies to the specific object. The policiesToAdd will be applied to the object and
   * the policiesToRemove will be removed from the object. Note that:
   *
   * <ol>
   *   <li>Adding or removing policies that are not existed will be ignored.
   *   <li>If the same name policy is in both policiesToAdd and policiesToRemove, it will be
   *       ignored.
   *   <li>If the policy is already applied to the object, it will throw {@link
   *       PolicyAlreadyAssociatedException}
   * </ol>
   *
   * @param policiesToAdd The policies to be added to the object.
   * @param policiesToRemove The policies to remove.
   * @return The list of applied policies.
   * @throws PolicyAlreadyAssociatedException If the policy is already applied to the object.
   */
  String[] associatePolicies(String[] policiesToAdd, String[] policiesToRemove)
      throws PolicyAlreadyAssociatedException;
}
