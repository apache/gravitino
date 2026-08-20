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
import static org.mockito.Mockito.when;

import java.util.Arrays;
import javax.annotation.Nullable;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.TagEventDispatcher;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.PolicyTagAssociationEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.PolicyTagSelector;
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
  private TagEntity tag;

  @BeforeEach
  void setUp() {
    listener = new DummyEventListener();
    delegate = mock(TagDispatcher.class);
    dispatcher = new TagEventDispatcher(new EventBus(Arrays.asList(listener)), delegate);
    policy = mock(PolicyEntity.class);
    tag = mock(TagEntity.class);
    when(policy.name()).thenReturn(POLICY);
    when(tag.name()).thenReturn(TAG);
  }

  @Test
  void testSetPolicyForTagEventsIncludePreviousRequestedAndResultingAssociation() {
    PolicyTagSelector previousSelector = PolicyTagSelector.tagValue("risk");
    PolicyTagSelector requestedSelector = PolicyTagSelector.tagValue("finance");
    PolicyTagAssociationEntity previous = association(previousSelector);
    PolicyTagAssociationEntity resulting = association(requestedSelector);
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new PolicyTagAssociationEntity[] {previous});
    when(delegate.setPolicyForTag(METALAKE, TAG, POLICY, requestedSelector)).thenReturn(resulting);

    Assertions.assertSame(
        resulting, dispatcher.setPolicyForTag(METALAKE, TAG, POLICY, requestedSelector));

    SetPolicyForTagPreEvent preEvent = (SetPolicyForTagPreEvent) listener.popPreEvent();
    Assertions.assertEquals(OperationType.SET_POLICY_FOR_TAG, preEvent.operationType());
    Assertions.assertEquals(METALAKE, preEvent.metalake());
    Assertions.assertEquals(TAG, preEvent.tagName());
    Assertions.assertEquals(POLICY, preEvent.policyName());
    Assertions.assertEquals(previousSelector, preEvent.previousSelector().orElseThrow());
    Assertions.assertEquals(requestedSelector, preEvent.requestedSelector().orElseThrow());

    SetPolicyForTagEvent event = (SetPolicyForTagEvent) listener.popPostEvent();
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(previousSelector, event.previousSelector().orElseThrow());
    Assertions.assertEquals(requestedSelector, event.requestedSelector().orElseThrow());
    Assertions.assertEquals(METALAKE, event.resultingAssociation().metalake());
    Assertions.assertEquals(TAG, event.resultingAssociation().tagName());
    Assertions.assertEquals(POLICY, event.resultingAssociation().policyName());
    Assertions.assertEquals(
        requestedSelector, event.resultingAssociation().selector().orElseThrow());
  }

  @Test
  void testSetPolicyForTagFailureEvent() {
    PolicyTagAssociationEntity previous = association(null);
    PolicyTagSelector requestedSelector = PolicyTagSelector.tagValue("finance");
    GravitinoRuntimeException exception = new GravitinoRuntimeException("set failed");
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new PolicyTagAssociationEntity[] {previous});
    when(delegate.setPolicyForTag(METALAKE, TAG, POLICY, requestedSelector)).thenThrow(exception);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> dispatcher.setPolicyForTag(METALAKE, TAG, POLICY, requestedSelector));

    SetPolicyForTagPreEvent preEvent = (SetPolicyForTagPreEvent) listener.popPreEvent();
    Assertions.assertTrue(preEvent.previousAssociation().isPresent());
    Assertions.assertTrue(preEvent.previousSelector().isEmpty());
    SetPolicyForTagFailureEvent event = (SetPolicyForTagFailureEvent) listener.popPostEvent();
    Assertions.assertSame(exception, event.exception());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertTrue(event.previousAssociation().isPresent());
    Assertions.assertTrue(event.previousSelector().isEmpty());
    Assertions.assertEquals(requestedSelector, event.requestedSelector().orElseThrow());
  }

  @Test
  void testRemovePolicyFromTagEventsIncludeRemovedAssociation() {
    PolicyTagSelector previousSelector = PolicyTagSelector.tagValue("finance");
    PolicyTagAssociationEntity previous = association(previousSelector);
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new PolicyTagAssociationEntity[] {previous});

    dispatcher.removePolicyFromTag(METALAKE, TAG, POLICY);

    RemovePolicyFromTagPreEvent preEvent = (RemovePolicyFromTagPreEvent) listener.popPreEvent();
    Assertions.assertEquals(OperationType.REMOVE_POLICY_FROM_TAG, preEvent.operationType());
    Assertions.assertEquals(previousSelector, preEvent.previousSelector().orElseThrow());
    RemovePolicyFromTagEvent event = (RemovePolicyFromTagEvent) listener.popPostEvent();
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(
        previousSelector, event.removedAssociation().orElseThrow().selector().orElseThrow());
  }

  @Test
  void testRemovePolicyFromTagIdempotentEvent() {
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new PolicyTagAssociationEntity[0]);

    dispatcher.removePolicyFromTag(METALAKE, TAG, POLICY);

    RemovePolicyFromTagPreEvent preEvent = (RemovePolicyFromTagPreEvent) listener.popPreEvent();
    Assertions.assertTrue(preEvent.previousAssociation().isEmpty());
    RemovePolicyFromTagEvent event = (RemovePolicyFromTagEvent) listener.popPostEvent();
    Assertions.assertTrue(event.removedAssociation().isEmpty());
  }

  @Test
  void testRemovePolicyFromTagFailureEvent() {
    PolicyTagAssociationEntity previous = association(PolicyTagSelector.tagValue("finance"));
    GravitinoRuntimeException exception = new GravitinoRuntimeException("remove failed");
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new PolicyTagAssociationEntity[] {previous});
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
    PolicyTagSelector selector = PolicyTagSelector.tagValue("finance");
    GravitinoRuntimeException exception = new GravitinoRuntimeException("list failed");
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG)).thenThrow(exception);

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> dispatcher.setPolicyForTag(METALAKE, TAG, POLICY, selector));

    SetPolicyForTagPreEvent preEvent = (SetPolicyForTagPreEvent) listener.popPreEvent();
    Assertions.assertTrue(preEvent.previousAssociation().isEmpty());
    Assertions.assertEquals(selector, preEvent.requestedSelector().orElseThrow());
    SetPolicyForTagFailureEvent event = (SetPolicyForTagFailureEvent) listener.popPostEvent();
    Assertions.assertSame(exception, event.exception());
  }

  private PolicyTagAssociationEntity association(@Nullable PolicyTagSelector selector) {
    return PolicyTagAssociationEntity.of(policy, tag, selector);
  }
}
