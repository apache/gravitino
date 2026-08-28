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

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.TagEventDispatcher;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.PolicyAssociationSelector;
import org.apache.gravitino.policy.TagValueSelector;
import org.apache.gravitino.tag.TagDispatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestPolicyTagRelationEvent {
  private static final String METALAKE = "metalake";
  private static final String TAG = "data_domain";
  private static final String POLICY = "retention";

  private DummyEventListener listener;
  private TagDispatcher delegate;
  private TagEventDispatcher dispatcher;
  private PolicyEntity policy;

  @BeforeEach
  void setUp() {
    listener = new DummyEventListener();
    delegate = mock(TagDispatcher.class);
    dispatcher = new TagEventDispatcher(new EventBus(Arrays.asList(listener)), delegate);
    policy = mock(PolicyEntity.class);
    when(policy.name()).thenReturn(POLICY);
  }

  @Test
  void testAddPolicyForTagEventsIncludeRequestedAndResultingAssociation() {
    PolicyAssociationSelector requestedSelector = TagValueSelector.of("finance");
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new RelationalEntity<?>[0]);

    dispatcher.addPolicyForTag(METALAKE, TAG, POLICY, requestedSelector);

    AddPolicyForTagPreEvent preEvent = (AddPolicyForTagPreEvent) listener.popPreEvent();
    Assertions.assertEquals(OperationType.ADD_POLICY_FOR_TAG, preEvent.operationType());
    Assertions.assertEquals(METALAKE, preEvent.metalake());
    Assertions.assertEquals(TAG, preEvent.tagName());
    Assertions.assertEquals(POLICY, preEvent.policyName());
    Assertions.assertTrue(preEvent.previousAssociation().isEmpty());
    Assertions.assertEquals(requestedSelector, preEvent.requestedSelector());

    AddPolicyForTagEvent event = (AddPolicyForTagEvent) listener.popPostEvent();
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertTrue(event.previousAssociation().isEmpty());
    Assertions.assertEquals(requestedSelector, event.requestedSelector());
    Assertions.assertEquals(METALAKE, event.resultingAssociation().metalake());
    Assertions.assertEquals(TAG, event.resultingAssociation().tagName());
    Assertions.assertEquals(POLICY, event.resultingAssociation().policyName());
    Assertions.assertEquals(requestedSelector, event.resultingAssociation().selector());
    verify(delegate, times(1)).listPolicyAssociationsForTag(METALAKE, TAG);
  }

  @Test
  void testAddPolicyForTagFailureEvent() {
    RelationalEntity<PolicyEntity> previous = association(AllValuesSelector.get());
    PolicyAssociationSelector requestedSelector = TagValueSelector.of("finance");
    GravitinoRuntimeException exception = new GravitinoRuntimeException("add failed");
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new RelationalEntity<?>[] {previous});
    doThrow(exception).when(delegate).addPolicyForTag(METALAKE, TAG, POLICY, requestedSelector);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> dispatcher.addPolicyForTag(METALAKE, TAG, POLICY, requestedSelector));

    AddPolicyForTagPreEvent preEvent = (AddPolicyForTagPreEvent) listener.popPreEvent();
    Assertions.assertTrue(preEvent.previousAssociation().isPresent());
    Assertions.assertSame(AllValuesSelector.get(), preEvent.previousSelector().orElseThrow());
    AddPolicyForTagFailureEvent event = (AddPolicyForTagFailureEvent) listener.popPostEvent();
    Assertions.assertSame(exception, event.exception());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertTrue(event.previousAssociation().isPresent());
    Assertions.assertSame(AllValuesSelector.get(), event.previousSelector().orElseThrow());
    Assertions.assertEquals(requestedSelector, event.requestedSelector());
  }

  @Test
  void testRemovePolicyFromTagEventsIncludeRemovedAssociation() {
    PolicyAssociationSelector previousSelector = TagValueSelector.of("finance");
    RelationalEntity<PolicyEntity> previous = association(previousSelector);
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new RelationalEntity<?>[] {previous});

    dispatcher.removePolicyFromTag(METALAKE, TAG, POLICY);

    RemovePolicyFromTagPreEvent preEvent = (RemovePolicyFromTagPreEvent) listener.popPreEvent();
    Assertions.assertEquals(OperationType.REMOVE_POLICY_FROM_TAG, preEvent.operationType());
    Assertions.assertEquals(previousSelector, preEvent.previousSelector().orElseThrow());
    RemovePolicyFromTagEvent event = (RemovePolicyFromTagEvent) listener.popPostEvent();
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(previousSelector, event.removedAssociation().orElseThrow().selector());
  }

  @Test
  void testRemovePolicyFromTagIdempotentEvent() {
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new RelationalEntity<?>[0]);

    dispatcher.removePolicyFromTag(METALAKE, TAG, POLICY);

    RemovePolicyFromTagPreEvent preEvent = (RemovePolicyFromTagPreEvent) listener.popPreEvent();
    Assertions.assertTrue(preEvent.previousAssociation().isEmpty());
    RemovePolicyFromTagEvent event = (RemovePolicyFromTagEvent) listener.popPostEvent();
    Assertions.assertTrue(event.removedAssociation().isEmpty());
  }

  @Test
  void testRemovePolicyFromTagFailureEvent() {
    RelationalEntity<PolicyEntity> previous = association(TagValueSelector.of("finance"));
    GravitinoRuntimeException exception = new GravitinoRuntimeException("remove failed");
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new RelationalEntity<?>[] {previous});
    doThrow(exception).when(delegate).removePolicyFromTag(METALAKE, TAG, POLICY);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> dispatcher.removePolicyFromTag(METALAKE, TAG, POLICY));

    RemovePolicyFromTagFailureEvent event =
        (RemovePolicyFromTagFailureEvent) listener.popPostEvent();
    Assertions.assertSame(exception, event.exception());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertTrue(event.previousAssociation().isPresent());
  }

  @Test
  void testRelationSnapshotFailureStillEmitsPreAndFailureEvents() {
    PolicyAssociationSelector selector = TagValueSelector.of("finance");
    GravitinoRuntimeException exception = new GravitinoRuntimeException("list failed");
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG)).thenThrow(exception);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> dispatcher.addPolicyForTag(METALAKE, TAG, POLICY, selector));

    AddPolicyForTagPreEvent preEvent = (AddPolicyForTagPreEvent) listener.popPreEvent();
    Assertions.assertTrue(preEvent.previousAssociation().isEmpty());
    Assertions.assertEquals(selector, preEvent.requestedSelector());
    AddPolicyForTagFailureEvent event = (AddPolicyForTagFailureEvent) listener.popPostEvent();
    Assertions.assertSame(exception, event.exception());
  }

  private RelationalEntity<PolicyEntity> association(PolicyAssociationSelector selector) {
    return new RelationalEntity<>(
        SupportsRelationOperations.Type.POLICY_TAG_REL,
        NameIdentifier.of(METALAKE, TAG),
        Entity.EntityType.TAG,
        policy,
        PolicyAssociationSelectorSerde.serialize(selector));
  }
}
