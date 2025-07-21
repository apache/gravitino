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

import java.util.Set;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.storage.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused") // todo: remove this when the methods are implemented
public class PolicyManager implements PolicyDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyManager.class);

  private final IdGenerator idGenerator;
  private final EntityStore entityStore;

  public PolicyManager(IdGenerator idGenerator, EntityStore entityStore) {
    if (!(entityStore instanceof SupportsRelationOperations)) {
      String errorMsg =
          "PolicyManager cannot run with entity store that does not support policy operations, "
              + "please configure the entity store to use relational entity store and restart the Gravitino server";
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    this.idGenerator = idGenerator;
    this.entityStore = entityStore;
  }

  @Override
  public String[] listPolicies(String metalake) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Policy[] listPolicyInfos(String metalake) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Policy getPolicy(String metalake, String policyName) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Policy createPolicy(
      String metalake,
      String policyName,
      String type,
      String comment,
      boolean enabled,
      boolean exclusive,
      boolean inheritable,
      Set<MetadataObject.Type> supportedObjectTypes,
      PolicyContent content) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Policy alterPolicy(String metalake, String policyName, PolicyChange... changes) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void enablePolicy(String metalake, String policyName) throws NoSuchPolicyException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void disablePolicy(String metalake, String policyName) throws NoSuchPolicyException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public boolean deletePolicy(String metalake, String policyName) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public MetadataObject[] listMetadataObjectsForPolicy(String metalake, String policyName) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public String[] listPoliciesForMetadataObject(String metalake, MetadataObject metadataObject) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Policy[] listPolicyInfosForMetadataObject(String metalake, MetadataObject metadataObject) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public String[] associatePoliciesForMetadataObject(
      String metalake,
      MetadataObject metadataObject,
      String[] policiesToAdd,
      String[] policiesToRemove) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Policy getPolicyForMetadataObject(
      String metalake, MetadataObject metadataObject, String policyName) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
