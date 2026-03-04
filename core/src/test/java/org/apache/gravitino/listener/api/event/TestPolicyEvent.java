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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Objects;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.PolicyEventDispatcher;
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
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyContent;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestPolicyEvent {
  private PolicyEventDispatcher failureDispatcher;
  private PolicyEventDispatcher dispatcher;
  private DummyEventListener dummyEventListener;
  private PolicyEntity policy;

  @BeforeAll
  void init() {
    this.policy = mockPolicy();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    PolicyDispatcher policyExceptionDispatcher = mockExceptionPolicyDispatcher();
    this.failureDispatcher = new PolicyEventDispatcher(eventBus, policyExceptionDispatcher);
    PolicyDispatcher policyDispatcher = mockPolicyDispatcher();
    this.dispatcher = new PolicyEventDispatcher(eventBus, policyDispatcher);
  }

  @Test
  void testListPoliciesEvent() {
    dispatcher.listPolicies("metalake");
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals("metalake", Objects.requireNonNull(preEvent.identifier()).toString());
    Assertions.assertEquals(ListPoliciesPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_POLICY, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals("metalake", Objects.requireNonNull(postevent.identifier()).toString());
    Assertions.assertEquals(ListPoliciesEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.LIST_POLICY, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testListPolicyInfosEvent() {
    dispatcher.listPolicyInfos("metalake");
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals("metalake", Objects.requireNonNull(preEvent.identifier()).toString());
    Assertions.assertEquals(ListPolicyInfosPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.LIST_POLICY_INFO, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals("metalake", Objects.requireNonNull(postevent.identifier()).toString());
    Assertions.assertEquals(ListPolicyInfosEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.LIST_POLICY_INFO, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testGetPolicyEvent() {
    dispatcher.getPolicy("metalake", policy.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofPolicy("metalake", policy.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(GetPolicyPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_POLICY, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();

    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(GetPolicyEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.GET_POLICY, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
    PolicyInfo policyInfo = ((GetPolicyEvent) postevent).policyInfo();
    checkPolicyInfo(policyInfo, policy);
  }

  @Test
  void testCreatePolicyEvent() {
    dispatcher.createPolicy(
        "metalake",
        policy.name(),
        policy.policyType(),
        policy.comment(),
        policy.enabled(),
        policy.content());
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofPolicy("metalake", policy.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(CreatePolicyPreEvent.class, preEvent.getClass());

    PolicyInfo policyInfo = ((CreatePolicyPreEvent) preEvent).createPolicyRequest();
    Assertions.assertEquals(policy.name(), policyInfo.name());
    Assertions.assertEquals(policy.policyType().name(), policyInfo.policyType());
    Assertions.assertEquals(policy.comment(), policyInfo.comment());
    Assertions.assertEquals(policy.enabled(), policyInfo.enabled());
    Assertions.assertEquals(policy.content(), policyInfo.content());

    Assertions.assertEquals(OperationType.CREATE_POLICY, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(CreatePolicyEvent.class, postevent.getClass());
    PolicyInfo policyInfo2 = ((CreatePolicyEvent) postevent).createdPolicyInfo();
    checkPolicyInfo(policyInfo2, policy);
    Assertions.assertEquals(OperationType.CREATE_POLICY, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testAlterPolicyEvent() {
    PolicyChange change1 = PolicyChange.rename("newName");
    PolicyChange[] changes = {change1};

    dispatcher.alterPolicy("metalake", policy.name(), changes);
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofPolicy("metalake", policy.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(AlterPolicyPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.ALTER_POLICY, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    PolicyChange[] eventChanges = ((AlterPolicyPreEvent) preEvent).policyChanges();
    Assertions.assertArrayEquals(changes, eventChanges);

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(AlterPolicyEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.ALTER_POLICY, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());

    PolicyChange[] postChanges = ((AlterPolicyEvent) postevent).policyChanges();
    Assertions.assertArrayEquals(changes, postChanges);
  }

  @Test
  void testEnablePolicyEvent() {
    dispatcher.enablePolicy("metalake", policy.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofPolicy("metalake", policy.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(EnablePolicyPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.ENABLE_POLICY, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(EnablePolicyEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.ENABLE_POLICY, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testDisablePolicyEvent() {
    dispatcher.disablePolicy("metalake", policy.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofPolicy("metalake", policy.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(DisablePolicyPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DISABLE_POLICY, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(DisablePolicyEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.DISABLE_POLICY, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testDeletePolicyEvent() {
    dispatcher.deletePolicy("metalake", policy.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofPolicy("metalake", policy.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(DeletePolicyPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.DELETE_POLICY, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(DeletePolicyEvent.class, postevent.getClass());
    Assertions.assertEquals(OperationType.DELETE_POLICY, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testListMetadataObjectsForPolicyEvent() {
    dispatcher.listMetadataObjectsForPolicy("metalake", policy.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = NameIdentifierUtil.ofPolicy("metalake", policy.name());

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(ListMetadataObjectsForPolicyPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(
        OperationType.LIST_METADATA_OBJECTS_FOR_POLICY, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(ListMetadataObjectsForPolicyEvent.class, postevent.getClass());
    Assertions.assertEquals(
        OperationType.LIST_METADATA_OBJECTS_FOR_POLICY, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testListPolicyInfosForMetadataObjectEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);
    dispatcher.listPolicyInfosForMetadataObject("metalake", metadataObject);

    PreEvent preEvent = dummyEventListener.popPreEvent();
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent("metalake", metadataObject);

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(ListPolicyInfosForMetadataObjectPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(
        OperationType.LIST_POLICY_INFOS_FOR_METADATA_OBJECT, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(ListPolicyInfosForMetadataObjectEvent.class, postevent.getClass());
    Assertions.assertEquals(
        OperationType.LIST_POLICY_INFOS_FOR_METADATA_OBJECT, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testAssociatePoliciesForMetadataObjectEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    String[] policiesToAdd = {"policy1", "policy2"};
    String[] policiesToRemove = {"policy3"};

    dispatcher.associatePoliciesForMetadataObject(
        "metalake", metadataObject, policiesToAdd, policiesToRemove);
    PreEvent preEvent = dummyEventListener.popPreEvent();

    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent("metalake", metadataObject);

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(AssociatePoliciesForMetadataObjectPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(
        MetadataObject.Type.CATALOG,
        ((AssociatePoliciesForMetadataObjectPreEvent) preEvent).metadataObject().type());
    Assertions.assertArrayEquals(
        policiesToAdd, ((AssociatePoliciesForMetadataObjectPreEvent) preEvent).policiesToAdd());
    Assertions.assertArrayEquals(
        policiesToRemove,
        ((AssociatePoliciesForMetadataObjectPreEvent) preEvent).policiesToRemove());

    Assertions.assertEquals(
        OperationType.ASSOCIATE_POLICIES_FOR_METADATA_OBJECT, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(AssociatePoliciesForMetadataObjectEvent.class, postevent.getClass());
    Assertions.assertEquals(
        MetadataObject.Type.CATALOG,
        ((AssociatePoliciesForMetadataObjectEvent) postevent).metadataObject().type());
    Assertions.assertArrayEquals(
        policiesToAdd, ((AssociatePoliciesForMetadataObjectEvent) postevent).policiesToAdd());
    Assertions.assertArrayEquals(
        policiesToRemove, ((AssociatePoliciesForMetadataObjectEvent) postevent).policiesToRemove());
    Assertions.assertEquals(
        OperationType.ASSOCIATE_POLICIES_FOR_METADATA_OBJECT, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());
  }

  @Test
  void testGetPolicyForMetadataObjectEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    dispatcher.getPolicyForMetadataObject("metalake", metadataObject, policy.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();

    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent("metalake", metadataObject);

    Assertions.assertEquals(identifier.toString(), preEvent.identifier().toString());
    Assertions.assertEquals(GetPolicyForMetadataObjectPreEvent.class, preEvent.getClass());
    Assertions.assertEquals(OperationType.GET_POLICY_FOR_METADATA_OBJECT, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postevent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(identifier.toString(), postevent.identifier().toString());
    Assertions.assertEquals(GetPolicyForMetadataObjectEvent.class, postevent.getClass());
    Assertions.assertEquals(
        OperationType.GET_POLICY_FOR_METADATA_OBJECT, postevent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postevent.operationStatus());

    PolicyInfo policyInfo = ((GetPolicyForMetadataObjectEvent) postevent).policyInfo();
    checkPolicyInfo(policyInfo, policy);
  }

  @Test
  void testCreatePolicyFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.createPolicy(
                "metalake",
                policy.name(),
                policy.policyType(),
                policy.comment(),
                policy.enabled(),
                policy.content()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(CreatePolicyFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((CreatePolicyFailureEvent) event).exception().getClass());
    Assertions.assertEquals(
        policy.name(), ((CreatePolicyFailureEvent) event).createPolicyRequest().name());
    Assertions.assertEquals(
        policy.policyType().name(),
        ((CreatePolicyFailureEvent) event).createPolicyRequest().policyType());
    Assertions.assertEquals(
        policy.comment(), ((CreatePolicyFailureEvent) event).createPolicyRequest().comment());
    Assertions.assertEquals(
        policy.enabled(), ((CreatePolicyFailureEvent) event).createPolicyRequest().enabled());
    Assertions.assertEquals(
        policy.content(), ((CreatePolicyFailureEvent) event).createPolicyRequest().content());
    Assertions.assertEquals(OperationType.CREATE_POLICY, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testGetPolicyFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getPolicy("metalake", policy.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetPolicyFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((GetPolicyFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_POLICY, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAlterPolicyFailureEvent() {
    PolicyChange change1 = PolicyChange.rename("newName");
    PolicyChange change2 = PolicyChange.updateComment("new comment");
    PolicyChange[] changes = new PolicyChange[] {change1, change2};
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterPolicy("metalake", policy.name(), changes));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AlterPolicyFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterPolicyFailureEvent) event).exception().getClass());
    Assertions.assertArrayEquals(changes, ((AlterPolicyFailureEvent) event).policyChanges());
    Assertions.assertEquals(OperationType.ALTER_POLICY, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testDeletePolicyFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.deletePolicy("metalake", policy.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DeletePolicyFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DeletePolicyFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DELETE_POLICY, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testEnablePolicyFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.enablePolicy("metalake", policy.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(EnablePolicyFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((EnablePolicyFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.ENABLE_POLICY, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testDisablePolicyFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.disablePolicy("metalake", policy.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(DisablePolicyFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((DisablePolicyFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DISABLE_POLICY, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListPoliciesFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listPolicies("metalake"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListPoliciesFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListPoliciesFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_POLICY, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListPolicyInfosFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listPolicyInfos("metalake"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListPolicyInfosFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListPolicyInfosFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_POLICY_INFO, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListMetadataObjectsForPolicyFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listMetadataObjectsForPolicy("metalake", policy.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListMetadataObjectsForPolicyFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListMetadataObjectsForPolicyFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_METADATA_OBJECTS_FOR_POLICY, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListPolicyInfosForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listPolicyInfosForMetadataObject("metalake", metadataObject));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(ListPolicyInfosForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListPolicyInfosForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(
        OperationType.LIST_POLICY_INFOS_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAssociatePoliciesForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    String[] policiesToAssociate = new String[] {"policy1", "policy2"};
    String[] policiesToDisassociate = new String[] {"policy3", "policy4"};

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.associatePoliciesForMetadataObject(
                "metalake", metadataObject, policiesToAssociate, policiesToDisassociate));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(AssociatePoliciesForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((AssociatePoliciesForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(
        MetadataObject.Type.CATALOG,
        ((AssociatePoliciesForMetadataObjectFailureEvent) event).metadataObject().type());
    Assertions.assertArrayEquals(
        policiesToAssociate,
        ((AssociatePoliciesForMetadataObjectFailureEvent) event).policiesToAdd());
    Assertions.assertArrayEquals(
        policiesToDisassociate,
        ((AssociatePoliciesForMetadataObjectFailureEvent) event).policiesToRemove());
    Assertions.assertEquals(
        OperationType.ASSOCIATE_POLICIES_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testGetPolicyForMetadataObjectFailureEvent() {
    MetadataObject metadataObject =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.getPolicyForMetadataObject(
                "metalake", metadataObject, policy.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertEquals(GetPolicyForMetadataObjectFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((GetPolicyForMetadataObjectFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_POLICY_FOR_METADATA_OBJECT, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  private PolicyEntity mockPolicy() {
    PolicyContent content =
        PolicyContents.custom(
            ImmutableMap.of("rule1", "value1"),
            ImmutableSet.of(MetadataObject.Type.CATALOG),
            ImmutableMap.of("property1", "value1"));

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(java.time.Instant.now()).build();

    return PolicyEntity.builder()
        .withId(1L)
        .withName("policy")
        .withPolicyType(Policy.BuiltInType.CUSTOM)
        .withComment("policy comment")
        .withEnabled(true)
        .withContent(content)
        .withAuditInfo(auditInfo)
        .build();
  }

  private PolicyDispatcher mockExceptionPolicyDispatcher() {
    return mock(
        PolicyDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }

  private void checkPolicyInfo(PolicyInfo actualPolicyInfo, PolicyEntity expectedPolicy) {
    Assertions.assertEquals(expectedPolicy.name(), actualPolicyInfo.name());
    Assertions.assertEquals(expectedPolicy.policyType().name(), actualPolicyInfo.policyType());
    Assertions.assertEquals(expectedPolicy.comment(), actualPolicyInfo.comment());
    Assertions.assertEquals(expectedPolicy.enabled(), actualPolicyInfo.enabled());
    Assertions.assertEquals(expectedPolicy.content(), actualPolicyInfo.content());
  }

  private PolicyDispatcher mockPolicyDispatcher() {
    PolicyDispatcher dispatcher = mock(PolicyDispatcher.class);
    String metalake = "metalake";
    String[] policyNames = new String[] {"policy1", "policy2"};
    PolicyEntity[] policies = new PolicyEntity[] {policy, policy};

    when(dispatcher.createPolicy(
            any(String.class),
            any(String.class),
            any(Policy.BuiltInType.class),
            any(String.class),
            any(Boolean.class),
            any(PolicyContent.class)))
        .thenReturn(policy);
    when(dispatcher.listPolicies(metalake)).thenReturn(policyNames);
    when(dispatcher.listPolicyInfos(metalake)).thenReturn(policies);
    when(dispatcher.alterPolicy(any(String.class), any(String.class), any(PolicyChange[].class)))
        .thenReturn(policy);
    when(dispatcher.getPolicy(any(String.class), any(String.class))).thenReturn(policy);
    when(dispatcher.deletePolicy(metalake, policy.name())).thenReturn(true);
    when(dispatcher.getPolicyForMetadataObject(
            any(String.class), any(MetadataObject.class), any(String.class)))
        .thenReturn(policy);
    MetadataObject catalog =
        NameIdentifierUtil.toMetadataObject(
            NameIdentifierUtil.ofCatalog("metalake", "catalog_for_test"),
            Entity.EntityType.CATALOG);
    MetadataObject[] objects = new MetadataObject[] {catalog};

    when(dispatcher.listMetadataObjectsForPolicy(any(String.class), any(String.class)))
        .thenReturn(objects);

    when(dispatcher.associatePoliciesForMetadataObject(
            any(String.class), any(MetadataObject.class), any(String[].class), any(String[].class)))
        .thenReturn(new String[] {"policy1", "policy2"});

    when(dispatcher.listPolicyInfosForMetadataObject(any(String.class), any(MetadataObject.class)))
        .thenReturn(new PolicyEntity[] {policy, policy});

    return dispatcher;
  }
}
