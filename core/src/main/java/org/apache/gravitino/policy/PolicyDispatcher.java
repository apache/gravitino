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

import java.util.Arrays;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * The interface provides functionalities for managing policies within a metalake. It includes a
 * comprehensive set of operations such as listing, creating, retrieving, updating, and deleting
 * policies, as well as associating policies with other objects.
 */
@Evolving
public interface PolicyDispatcher {

  /**
   * List all the policy names under a metalake.
   *
   * @param metalake the name of the metalake
   * @return The list of policy names.
   */
  String[] listPolicies(String metalake);

  /**
   * List all the policies with detailed information under a metalake.
   *
   * @param metalake the name of the metalake
   * @return The list of policies.
   */
  PolicyEntity[] listPolicyInfos(String metalake);

  /**
   * Get a policy by its name under a metalake.
   *
   * @param metalake the name of the metalake
   * @param policyName the name of the policy
   * @return The policy.
   * @throws NoSuchPolicyException If the policy does not exist.
   */
  PolicyEntity getPolicy(String metalake, String policyName) throws NoSuchPolicyException;

  /**
   * Create a built-in type policy under a metalake. The exclusive, inheritable, and supported
   * object types parameters attributes are determined by the type of the policy.
   *
   * @param metalake the name of the metalake
   * @param name The name of the policy
   * @param type The type of the policy
   * @param comment The comment of the policy
   * @param enabled Whether the policy is enabled or not
   * @param content The content of the policy
   * @return The created policy
   * @throws PolicyAlreadyExistsException If the policy already exists
   */
  PolicyEntity createPolicy(
      String metalake,
      String name,
      Policy.BuiltInType type,
      String comment,
      boolean enabled,
      PolicyContent content)
      throws PolicyAlreadyExistsException;

  /**
   * Alter an existing policy under a metalake.
   *
   * @param metalake the name of the metalake
   * @param policyName the name of the policy
   * @param changes the changes to apply to the policy
   * @return The updated policy.
   */
  PolicyEntity alterPolicy(String metalake, String policyName, PolicyChange... changes);

  /**
   * Enable an existing policy under a metalake.
   *
   * @param metalake the name of the metalake
   * @param policyName the name of the policy
   */
  void enablePolicy(String metalake, String policyName) throws NoSuchPolicyException;

  /**
   * Disable an existing policy under a metalake.
   *
   * @param metalake the name of the metalake
   * @param policyName the name of the policy
   */
  void disablePolicy(String metalake, String policyName) throws NoSuchPolicyException;

  /**
   * Delete an existing policy under a metalake.
   *
   * @param metalake the name of the metalake
   * @param policyName the name of the policy
   * @return True if the policy was successfully deleted, false otherwise.
   */
  boolean deletePolicy(String metalake, String policyName);

  /**
   * List all metadata objects associated with the specified policy under a metalake.
   *
   * @param metalake the name of the metalake
   * @param policyName the name of the policy
   * @return The array of metadata objects associated with the specified policy.
   */
  MetadataObject[] listMetadataObjectsForPolicy(String metalake, String policyName);

  /**
   * List all the policy names associated with a metadata object under a metalake.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which associated policies
   * @return The array of policy names associated with the specified metadata object.
   */
  default String[] listPoliciesForMetadataObject(String metalake, MetadataObject metadataObject) {
    return Arrays.stream(listPolicyInfosForMetadataObject(metalake, metadataObject))
        .map(PolicyEntity::name)
        .toArray(String[]::new);
  }

  /**
   * List all the policies with detailed information associated with a metadata object under a
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which associated policies
   * @return The array of policies associated with the specified metadata object.
   */
  PolicyEntity[] listPolicyInfosForMetadataObject(String metalake, MetadataObject metadataObject);

  /**
   * Associate policies to a metadata object under a metalake.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object to associate policies with
   * @param policiesToAdd the policies to be added to the metadata object
   * @param policiesToRemove the policies to remove from the metadata object
   * @return An array of updated policy names.
   */
  String[] associatePoliciesForMetadataObject(
      String metalake,
      MetadataObject metadataObject,
      String[] policiesToAdd,
      String[] policiesToRemove);

  /**
   * Get a specific policy associated with the specified metadata object.
   *
   * @param metalake the name of the metalake
   * @param metadataObject the metadata object for which to retrieve the policy
   * @param policyName the name of the policy to retrieve
   * @return The policy associated with the metadata object.
   */
  PolicyEntity getPolicyForMetadataObject(
      String metalake, MetadataObject metadataObject, String policyName);
}
