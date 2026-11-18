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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.json.PolicyAssociationSelectorSerde;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.TagEventDispatcher;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.policy.PolicyAssociationSelector;
import org.apache.gravitino.policy.TagValueSelector;
import org.apache.gravitino.tag.TagDispatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class TestPolicyTagRelationEvent {
  private static final String METALAKE = "metalake";
  private static final String TAG = "data_domain";
  private static final String POLICY = "retention";

  private DummyEventListener listener;
  private TagDispatcher delegate;
  private TagEventDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    listener = new DummyEventListener();
    delegate = mock(TagDispatcher.class);
    dispatcher = new TagEventDispatcher(new EventBus(Arrays.asList(listener)), delegate);
  }

  @Test
  void testAddPolicyForTagEventsContainMutationIntent() {
    PolicyAssociationSelector selector = TagValueSelector.of("finance");

    dispatcher.addPolicyForTag(METALAKE, TAG, POLICY, selector);

    AddPolicyForTagPreEvent preEvent = (AddPolicyForTagPreEvent) listener.popPreEvent();
    assertAddEventFields(preEvent.metalake(), preEvent.tagName(), preEvent.policyName());
    Assertions.assertEquals(OperationType.ADD_POLICY_FOR_TAG, preEvent.operationType());
    Assertions.assertEquals(selector, preEvent.requestedSelector());

    AddPolicyForTagEvent event = (AddPolicyForTagEvent) listener.popPostEvent();
    assertAddEventFields(event.metalake(), event.tagName(), event.policyName());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(selector, event.requestedSelector());
    Assertions.assertEquals(POLICY, event.customInfo().get("policyName"));
    Assertions.assertEquals(
        PolicyAssociationSelectorSerde.serialize(selector), event.customInfo().get("selector"));
    verify(delegate).addPolicyForTag(METALAKE, TAG, POLICY, selector);
    verify(delegate, never()).listPolicyAssociationsForTag(METALAKE, TAG);
  }

  @Test
  void testAddPolicyForTagFailureEvent() {
    PolicyAssociationSelector selector = TagValueSelector.of("finance");
    GravitinoRuntimeException exception = new GravitinoRuntimeException("add failed");
    doThrow(exception).when(delegate).addPolicyForTag(METALAKE, TAG, POLICY, selector);

    Assertions.assertSame(
        exception,
        Assertions.assertThrowsExactly(
            GravitinoRuntimeException.class,
            () -> dispatcher.addPolicyForTag(METALAKE, TAG, POLICY, selector)));

    AddPolicyForTagPreEvent preEvent = (AddPolicyForTagPreEvent) listener.popPreEvent();
    Assertions.assertEquals(selector, preEvent.requestedSelector());
    AddPolicyForTagFailureEvent event = (AddPolicyForTagFailureEvent) listener.popPostEvent();
    Assertions.assertSame(exception, event.exception());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(selector, event.requestedSelector());
    Assertions.assertEquals(POLICY, event.customInfo().get("policyName"));
    verify(delegate, never()).listPolicyAssociationsForTag(METALAKE, TAG);
  }

  @Test
  void testRemovePolicyFromTagEventsContainMutationIntent() {
    dispatcher.removePolicyFromTag(METALAKE, TAG, POLICY);

    RemovePolicyFromTagPreEvent preEvent = (RemovePolicyFromTagPreEvent) listener.popPreEvent();
    assertRemoveEventFields(preEvent.metalake(), preEvent.tagName(), preEvent.policyName());
    Assertions.assertEquals(OperationType.REMOVE_POLICY_FROM_TAG, preEvent.operationType());

    RemovePolicyFromTagEvent event = (RemovePolicyFromTagEvent) listener.popPostEvent();
    assertRemoveEventFields(event.metalake(), event.tagName(), event.policyName());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertEquals(POLICY, event.customInfo().get("policyName"));
    verify(delegate).removePolicyFromTag(METALAKE, TAG, POLICY);
    verify(delegate, never()).listPolicyAssociationsForTag(METALAKE, TAG);
  }

  @Test
  void testRemovePolicyFromTagFailureEvent() {
    GravitinoRuntimeException exception = new GravitinoRuntimeException("remove failed");
    doThrow(exception).when(delegate).removePolicyFromTag(METALAKE, TAG, POLICY);

    Assertions.assertSame(
        exception,
        Assertions.assertThrowsExactly(
            GravitinoRuntimeException.class,
            () -> dispatcher.removePolicyFromTag(METALAKE, TAG, POLICY)));

    RemovePolicyFromTagFailureEvent event =
        (RemovePolicyFromTagFailureEvent) listener.popPostEvent();
    Assertions.assertSame(exception, event.exception());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
    Assertions.assertEquals(POLICY, event.policyName());
    verify(delegate, never()).listPolicyAssociationsForTag(METALAKE, TAG);
  }

  @Test
  void testPreEventIsDispatchedBeforeMutation() {
    EventListenerPlugin orderedListener = mock(EventListenerPlugin.class);
    TagEventDispatcher orderedDispatcher =
        new TagEventDispatcher(new EventBus(Collections.singletonList(orderedListener)), delegate);
    PolicyAssociationSelector selector = TagValueSelector.of("finance");

    orderedDispatcher.addPolicyForTag(METALAKE, TAG, POLICY, selector);

    InOrder ordered = inOrder(orderedListener, delegate);
    ordered.verify(orderedListener).onPreEvent(any(AddPolicyForTagPreEvent.class));
    ordered.verify(delegate).addPolicyForTag(METALAKE, TAG, POLICY, selector);
    ordered.verify(orderedListener).onPostEvent(any(AddPolicyForTagEvent.class));
  }

  @Test
  void testPreEventVetoPreventsBusinessAccess() {
    EventListenerPlugin vetoListener = mock(EventListenerPlugin.class);
    ForbiddenException forbidden = new ForbiddenException("denied");
    doThrow(forbidden).when(vetoListener).onPreEvent(any(AddPolicyForTagPreEvent.class));
    TagEventDispatcher vetoDispatcher =
        new TagEventDispatcher(new EventBus(Collections.singletonList(vetoListener)), delegate);

    Assertions.assertSame(
        forbidden,
        Assertions.assertThrowsExactly(
            ForbiddenException.class,
            () ->
                vetoDispatcher.addPolicyForTag(
                    METALAKE, TAG, POLICY, TagValueSelector.of("finance"))));
    verify(delegate, never())
        .addPolicyForTag(
            any(String.class),
            any(String.class),
            any(String.class),
            any(PolicyAssociationSelector.class));
    verify(delegate, never()).listPolicyAssociationsForTag(any(String.class), any(String.class));
  }

  private static void assertAddEventFields(String metalake, String tagName, String policyName) {
    Assertions.assertEquals(METALAKE, metalake);
    Assertions.assertEquals(TAG, tagName);
    Assertions.assertEquals(POLICY, policyName);
  }

  private static void assertRemoveEventFields(String metalake, String tagName, String policyName) {
    assertAddEventFields(metalake, tagName, policyName);
  }
}
