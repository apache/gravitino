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

import java.io.IOException;
import java.util.List;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * An interface to support extra policy operations, this interface should be mixed with {@link
 * EntityStore} to provide extra operations.
 *
 * <p>Any operations that can be done by the entity store should be added here.
 */
public interface SupportsPolicyOperations {
  /**
   * List all the metadata objects that are associated with the given policy.
   *
   * @param policyIdent The identifier of the policy.
   * @return The list of metadata objects associated with the given policy.
   * @throws NoSuchEntityException if the policy does not exist.
   * @throws IOException If an error occurs while accessing the entity store.
   */
  List<MetadataObject> listAssociatedMetadataObjectsForPolicy(NameIdentifier policyIdent)
      throws NoSuchEntityException, IOException;

  /**
   * List all the policies that are associated with the given metadata object.
   *
   * @param objectIdent the identifier of the metadata object
   * @param objectType the type of the metadata object
   * @return The list of policies associated with the given metadata object.
   * @throws NoSuchEntityException if the metadata object does not exist.
   * @throws IOException If an error occurs while accessing the entity store.
   */
  List<PolicyEntity> listAssociatedPoliciesForMetadataObject(
      NameIdentifier objectIdent, MetadataObject.Type objectType)
      throws NoSuchEntityException, IOException;

  /**
   * Get the policy with the given identifier that is associated with the given metadata object.
   *
   * @param objectIdent the identifier of the metadata object
   * @param objectType the type of the metadata object
   * @param policyIdent the identifier of the policy
   * @return The policy associated with the metadata object.
   * @throws NoSuchEntityException if the metadata object does not exist or the policy is not
   * @throws IOException If an error occurs while accessing the entity store.
   */
  PolicyEntity getPolicyForMetadataObject(
      NameIdentifier objectIdent, MetadataObject.Type objectType, NameIdentifier policyIdent)
      throws NoSuchEntityException, IOException;

  /**
   * Associate policies with a metadata object.
   *
   * @param objectIdent the identifier of the metadata object
   * @param objectType the type of the metadata object
   * @param policiesToAdd the policies to add
   * @param policiesToRemove the policies to remove
   * @return The list of updated policy entities associated with the metadata object.
   * @throws NoSuchEntityException if the metadata object does not exist or the policies to add or
   *     remove do not exist.
   * @throws EntityAlreadyExistsException if the policies to add already exist for the metadata
   *     object.
   * @throws IOException if an error occurs while accessing the entity store.
   */
  List<PolicyEntity> associatePoliciesWithMetadataObject(
      NameIdentifier objectIdent,
      MetadataObject.Type objectType,
      NameIdentifier[] policiesToAdd,
      NameIdentifier[] policiesToRemove)
      throws NoSuchEntityException, EntityAlreadyExistsException, IOException;
}
