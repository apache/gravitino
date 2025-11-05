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
package org.apache.gravitino.listener;

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.listener.api.event.policy.AlterPolicyEvent;
import org.apache.gravitino.listener.api.event.policy.AlterPolicyFailureEvent;
import org.apache.gravitino.listener.api.event.policy.AlterPolicyPreEvent;
import org.apache.gravitino.listener.api.event.policy.AssociatePoliciesForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.policy.AssociatePoliciesForMetadataObjectFailureEvent;
import org.apache.gravitino.listener.api.event.policy.AssociatePoliciesForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.policy.CreatePolicyEvent;
import org.apache.gravitino.listener.api.event.policy.CreatePolicyFailureEvent;
import org.apache.gravitino.listener.api.event.policy.CreatePolicyPreEvent;
import org.apache.gravitino.listener.api.event.policy.DeletePolicyEvent;
import org.apache.gravitino.listener.api.event.policy.DeletePolicyFailureEvent;
import org.apache.gravitino.listener.api.event.policy.DeletePolicyPreEvent;
import org.apache.gravitino.listener.api.event.policy.DisablePolicyEvent;
import org.apache.gravitino.listener.api.event.policy.DisablePolicyFailureEvent;
import org.apache.gravitino.listener.api.event.policy.DisablePolicyPreEvent;
import org.apache.gravitino.listener.api.event.policy.EnablePolicyEvent;
import org.apache.gravitino.listener.api.event.policy.EnablePolicyFailureEvent;
import org.apache.gravitino.listener.api.event.policy.EnablePolicyPreEvent;
import org.apache.gravitino.listener.api.event.policy.GetPolicyEvent;
import org.apache.gravitino.listener.api.event.policy.GetPolicyFailureEvent;
import org.apache.gravitino.listener.api.event.policy.GetPolicyForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.policy.GetPolicyForMetadataObjectFailureEvent;
import org.apache.gravitino.listener.api.event.policy.GetPolicyForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.policy.GetPolicyPreEvent;
import org.apache.gravitino.listener.api.event.policy.ListMetadataObjectsForPolicyEvent;
import org.apache.gravitino.listener.api.event.policy.ListMetadataObjectsForPolicyFailureEvent;
import org.apache.gravitino.listener.api.event.policy.ListMetadataObjectsForPolicyPreEvent;
import org.apache.gravitino.listener.api.event.policy.ListPoliciesEvent;
import org.apache.gravitino.listener.api.event.policy.ListPoliciesFailureEvent;
import org.apache.gravitino.listener.api.event.policy.ListPoliciesPreEvent;
import org.apache.gravitino.listener.api.event.policy.ListPolicyInfosEvent;
import org.apache.gravitino.listener.api.event.policy.ListPolicyInfosFailureEvent;
import org.apache.gravitino.listener.api.event.policy.ListPolicyInfosForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.policy.ListPolicyInfosForMetadataObjectFailureEvent;
import org.apache.gravitino.listener.api.event.policy.ListPolicyInfosForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.policy.ListPolicyInfosPreEvent;
import org.apache.gravitino.listener.api.info.PolicyInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code PolicyEventDispatcher} is a decorator for {@link PolicyDispatcher} that not only delegates
 * policy operations to the underlying policy dispatcher but also dispatches corresponding events to
 * an {@link EventBus} after each operation is completed. This allows for event-driven workflows or
 * monitoring of policy operations.
 */
public class PolicyEventDispatcher implements PolicyDispatcher {
  private final EventBus eventBus;
  private final PolicyDispatcher dispatcher;

  public PolicyEventDispatcher(EventBus eventBus, PolicyDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public String[] listPolicies(String metalake) {
    eventBus.dispatchEvent(new ListPoliciesPreEvent(PrincipalUtils.getCurrentUserName(), metalake));
    try {
      String[] policyNames = dispatcher.listPolicies(metalake);
      eventBus.dispatchEvent(new ListPoliciesEvent(PrincipalUtils.getCurrentUserName(), metalake));
      return policyNames;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListPoliciesFailureEvent(PrincipalUtils.getCurrentUserName(), metalake, e));
      throw e;
    }
  }

  @Override
  public PolicyEntity[] listPolicyInfos(String metalake) {
    eventBus.dispatchEvent(
        new ListPolicyInfosPreEvent(PrincipalUtils.getCurrentUserName(), metalake));
    try {
      PolicyEntity[] policies = dispatcher.listPolicyInfos(metalake);
      eventBus.dispatchEvent(
          new ListPolicyInfosEvent(PrincipalUtils.getCurrentUserName(), metalake));
      return policies;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListPolicyInfosFailureEvent(PrincipalUtils.getCurrentUserName(), metalake, e));
      throw e;
    }
  }

  @Override
  public PolicyEntity getPolicy(String metalake, String policyName) throws NoSuchPolicyException {
    eventBus.dispatchEvent(
        new GetPolicyPreEvent(
            PrincipalUtils.getCurrentUserName(),
            NameIdentifierUtil.ofPolicy(metalake, policyName)));
    try {
      PolicyEntity policy = dispatcher.getPolicy(metalake, policyName);
      PolicyInfo policyInfo = toPolicyInfo(policy);
      eventBus.dispatchEvent(
          new GetPolicyEvent(
              PrincipalUtils.getCurrentUserName(),
              NameIdentifierUtil.ofPolicy(metalake, policyName),
              policyInfo));
      return policy;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new GetPolicyFailureEvent(
              PrincipalUtils.getCurrentUserName(),
              NameIdentifierUtil.ofPolicy(metalake, policyName),
              e));
      throw e;
    }
  }

  @Override
  public PolicyEntity createPolicy(
      String metalake,
      String name,
      Policy.BuiltInType type,
      String comment,
      boolean enabled,
      PolicyContent content) {
    PolicyInfo policyInfo = new PolicyInfo(name, type.name(), comment, enabled, content, null);
    eventBus.dispatchEvent(
        new CreatePolicyPreEvent(
            PrincipalUtils.getCurrentUserName(),
            NameIdentifierUtil.ofPolicy(metalake, name),
            policyInfo));
    try {
      PolicyEntity policy =
          dispatcher.createPolicy(metalake, name, type, comment, enabled, content);
      eventBus.dispatchEvent(
          new CreatePolicyEvent(
              PrincipalUtils.getCurrentUserName(),
              NameIdentifierUtil.ofPolicy(metalake, name),
              toPolicyInfo(policy)));
      return policy;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new CreatePolicyFailureEvent(
              PrincipalUtils.getCurrentUserName(),
              NameIdentifierUtil.ofPolicy(metalake, name),
              e,
              policyInfo));
      throw e;
    }
  }

  @Override
  public PolicyEntity alterPolicy(String metalake, String policyName, PolicyChange... changes) {
    AlterPolicyPreEvent preEvent =
        new AlterPolicyPreEvent(
            PrincipalUtils.getCurrentUserName(),
            NameIdentifierUtil.ofPolicy(metalake, policyName),
            changes);

    eventBus.dispatchEvent(preEvent);
    try {
      PolicyEntity policy = dispatcher.alterPolicy(metalake, policyName, changes);
      eventBus.dispatchEvent(
          new AlterPolicyEvent(
              PrincipalUtils.getCurrentUserName(),
              NameIdentifierUtil.ofPolicy(metalake, policyName),
              changes,
              toPolicyInfo(policy)));
      return policy;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterPolicyFailureEvent(
              PrincipalUtils.getCurrentUserName(),
              NameIdentifierUtil.ofPolicy(metalake, policyName),
              e,
              changes));
      throw e;
    }
  }

  @Override
  public void enablePolicy(String metalake, String policyName) throws NoSuchPolicyException {
    NameIdentifier identifier = NameIdentifierUtil.ofPolicy(metalake, policyName);
    eventBus.dispatchEvent(
        new EnablePolicyPreEvent(PrincipalUtils.getCurrentUserName(), identifier));
    try {
      dispatcher.enablePolicy(metalake, policyName);
      eventBus.dispatchEvent(
          new EnablePolicyEvent(PrincipalUtils.getCurrentUserName(), identifier));
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new EnablePolicyFailureEvent(PrincipalUtils.getCurrentUserName(), identifier, e));
      throw e;
    }
  }

  @Override
  public void disablePolicy(String metalake, String policyName) throws NoSuchPolicyException {
    NameIdentifier identifier = NameIdentifierUtil.ofPolicy(metalake, policyName);
    eventBus.dispatchEvent(
        new DisablePolicyPreEvent(PrincipalUtils.getCurrentUserName(), identifier));
    try {
      dispatcher.disablePolicy(metalake, policyName);
      eventBus.dispatchEvent(
          new DisablePolicyEvent(PrincipalUtils.getCurrentUserName(), identifier));
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DisablePolicyFailureEvent(PrincipalUtils.getCurrentUserName(), identifier, e));
      throw e;
    }
  }

  @Override
  public boolean deletePolicy(String metalake, String policyName) {
    DeletePolicyPreEvent preEvent =
        new DeletePolicyPreEvent(
            PrincipalUtils.getCurrentUserName(), NameIdentifierUtil.ofPolicy(metalake, policyName));

    eventBus.dispatchEvent(preEvent);
    try {
      boolean isExists = dispatcher.deletePolicy(metalake, policyName);
      eventBus.dispatchEvent(
          new DeletePolicyEvent(
              PrincipalUtils.getCurrentUserName(),
              NameIdentifierUtil.ofPolicy(metalake, policyName),
              isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DeletePolicyFailureEvent(
              PrincipalUtils.getCurrentUserName(),
              NameIdentifierUtil.ofPolicy(metalake, policyName),
              e));
      throw e;
    }
  }

  @Override
  public MetadataObject[] listMetadataObjectsForPolicy(String metalake, String policyName) {
    eventBus.dispatchEvent(
        new ListMetadataObjectsForPolicyPreEvent(
            PrincipalUtils.getCurrentUserName(),
            NameIdentifierUtil.ofPolicy(metalake, policyName)));
    try {
      MetadataObject[] metadataObjects =
          dispatcher.listMetadataObjectsForPolicy(metalake, policyName);
      eventBus.dispatchEvent(
          new ListMetadataObjectsForPolicyEvent(
              PrincipalUtils.getCurrentUserName(),
              NameIdentifierUtil.ofPolicy(metalake, policyName)));
      return metadataObjects;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListMetadataObjectsForPolicyFailureEvent(
              PrincipalUtils.getCurrentUserName(),
              NameIdentifierUtil.ofPolicy(metalake, policyName),
              e));
      throw e;
    }
  }

  @Override
  public PolicyEntity[] listPolicyInfosForMetadataObject(
      String metalake, MetadataObject metadataObject) {
    eventBus.dispatchEvent(
        new ListPolicyInfosForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(), metalake, metadataObject));
    try {
      PolicyEntity[] policies =
          dispatcher.listPolicyInfosForMetadataObject(metalake, metadataObject);
      eventBus.dispatchEvent(
          new ListPolicyInfosForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(), metalake, metadataObject));
      return policies;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListPolicyInfosForMetadataObjectFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, metadataObject, e));
      throw e;
    }
  }

  @Override
  public String[] associatePoliciesForMetadataObject(
      String metalake,
      MetadataObject metadataObject,
      String[] policiesToAdd,
      String[] policiesToRemove) {
    eventBus.dispatchEvent(
        new AssociatePoliciesForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(),
            metalake,
            metadataObject,
            policiesToAdd,
            policiesToRemove));

    try {
      String[] associatedPolicies =
          dispatcher.associatePoliciesForMetadataObject(
              metalake, metadataObject, policiesToAdd, policiesToRemove);
      eventBus.dispatchEvent(
          new AssociatePoliciesForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              metadataObject,
              policiesToAdd,
              policiesToRemove));
      return associatedPolicies;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AssociatePoliciesForMetadataObjectFailureEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              metadataObject,
              policiesToAdd,
              policiesToRemove,
              e));
      throw e;
    }
  }

  @Override
  public PolicyEntity getPolicyForMetadataObject(
      String metalake, MetadataObject metadataObject, String policyName) {
    eventBus.dispatchEvent(
        new GetPolicyForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(), metalake, metadataObject, policyName));
    try {
      PolicyEntity policy =
          dispatcher.getPolicyForMetadataObject(metalake, metadataObject, policyName);
      PolicyInfo policyInfo = toPolicyInfo(policy);
      eventBus.dispatchEvent(
          new GetPolicyForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(), metalake, metadataObject, policyInfo));
      return policy;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new GetPolicyForMetadataObjectFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, metadataObject, policyName, e));
      throw e;
    }
  }

  private PolicyInfo toPolicyInfo(PolicyEntity policy) {
    return new PolicyInfo(
        policy.name(),
        policy.policyType().name(),
        policy.comment(),
        policy.enabled(),
        policy.content(),
        policy.auditInfo());
  }
}
