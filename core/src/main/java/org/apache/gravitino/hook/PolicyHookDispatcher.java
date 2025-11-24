/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.hook;

import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.PolicyAlreadyExistsException;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

public class PolicyHookDispatcher implements PolicyDispatcher {

  private final PolicyDispatcher dispatcher;

  public PolicyHookDispatcher(PolicyDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public String[] listPolicies(String metalake) {
    return dispatcher.listPolicies(metalake);
  }

  @Override
  public PolicyEntity[] listPolicyInfos(String metalake) {
    return dispatcher.listPolicyInfos(metalake);
  }

  @Override
  public PolicyEntity getPolicy(String metalake, String policyName) throws NoSuchPolicyException {
    return dispatcher.getPolicy(metalake, policyName);
  }

  @Override
  public PolicyEntity createPolicy(
      String metalake,
      String name,
      Policy.BuiltInType type,
      String comment,
      boolean enabled,
      PolicyContent content)
      throws PolicyAlreadyExistsException {
    AuthorizationUtils.checkCurrentUser(metalake, PrincipalUtils.getCurrentUserName());
    PolicyEntity policy = dispatcher.createPolicy(metalake, name, type, comment, enabled, content);

    // Set the creator as the owner of the catalog.
    OwnerDispatcher ownerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerDispatcher != null) {
      ownerDispatcher.setOwner(
          metalake,
          NameIdentifierUtil.toMetadataObject(
              NameIdentifierUtil.ofPolicy(metalake, name), Entity.EntityType.TAG),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
    return policy;
  }

  @Override
  public PolicyEntity alterPolicy(String metalake, String policyName, PolicyChange... changes) {
    return dispatcher.alterPolicy(metalake, policyName, changes);
  }

  @Override
  public void enablePolicy(String metalake, String policyName) throws NoSuchPolicyException {
    dispatcher.enablePolicy(metalake, policyName);
  }

  @Override
  public void disablePolicy(String metalake, String policyName) throws NoSuchPolicyException {
    dispatcher.disablePolicy(metalake, policyName);
  }

  @Override
  public boolean deletePolicy(String metalake, String policyName) {
    return dispatcher.deletePolicy(metalake, policyName);
  }

  @Override
  public MetadataObject[] listMetadataObjectsForPolicy(String metalake, String policyName) {
    return dispatcher.listMetadataObjectsForPolicy(metalake, policyName);
  }

  @Override
  public PolicyEntity[] listPolicyInfosForMetadataObject(
      String metalake, MetadataObject metadataObject) {
    return dispatcher.listPolicyInfosForMetadataObject(metalake, metadataObject);
  }

  @Override
  public String[] associatePoliciesForMetadataObject(
      String metalake,
      MetadataObject metadataObject,
      String[] policiesToAdd,
      String[] policiesToRemove) {
    return dispatcher.associatePoliciesForMetadataObject(
        metalake, metadataObject, policiesToAdd, policiesToRemove);
  }

  @Override
  public PolicyEntity getPolicyForMetadataObject(
      String metalake, MetadataObject metadataObject, String policyName) {
    return dispatcher.getPolicyForMetadataObject(metalake, metadataObject, policyName);
  }
}
