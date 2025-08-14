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
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;

/**
 * The interface of the policy operations. The policy operations are used to manage policies under a
 * metalake. This interface will be mixed with GravitinoMetalake or GravitinoClient to provide
 * policy operations.
 */
@Evolving
public interface PolicyOperations {

  /**
   * List all the policy names under a metalake.
   *
   * @return The list of policy names.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  String[] listPolicies() throws NoSuchMetalakeException;

  /**
   * List all the policies with detailed information under a metalake.
   *
   * @return The list of policies.
   * @throws NoSuchMetalakeException If the metalake does not exist.
   */
  Policy[] listPolicyInfos() throws NoSuchMetalakeException;

  /**
   * Get a policy by its name under a metalake.
   *
   * @param name The name of the policy.
   * @return The policy.
   * @throws NoSuchPolicyException If the policy does not exist.
   */
  Policy getPolicy(String name) throws NoSuchPolicyException;

  /**
   * Create a policy under a metalake.
   *
   * @param name The name of the policy.
   * @param type The type of the policy.
   * @param comment The comment of the policy.
   * @param enabled Whether the policy is enabled or not.
   * @param content The content of the policy.
   * @return The created policy.
   * @throws PolicyAlreadyExistsException If the policy already exists.
   */
  Policy createPolicy(
      String name, String type, String comment, boolean enabled, PolicyContent content)
      throws PolicyAlreadyExistsException;

  /**
   * Enable a policy under a metalake. If the policy is already enabled, this method does nothing.
   *
   * @param name The name of the policy to enable.
   * @throws NoSuchPolicyException If the policy does not exist.
   */
  void enablePolicy(String name) throws NoSuchPolicyException;

  /**
   * Disable a policy under a metalake. If the policy is already disabled, this method does nothing.
   *
   * @param name The name of the policy to disable.
   * @throws NoSuchPolicyException If the policy does not exist.
   */
  void disablePolicy(String name) throws NoSuchPolicyException;

  /**
   * Alter a policy under a metalake.
   *
   * @param name The name of the policy.
   * @param changes The changes to apply to the policy.
   * @return The altered policy.
   * @throws NoSuchPolicyException If the policy does not exist.
   * @throws IllegalArgumentException If the changes cannot be associated with the policy.
   */
  Policy alterPolicy(String name, PolicyChange... changes)
      throws NoSuchPolicyException, IllegalArgumentException;

  /**
   * Delete a policy under a metalake.
   *
   * @param name The name of the policy.
   * @return True if the policy is deleted, false if the policy does not exist.
   */
  boolean deletePolicy(String name);
}
