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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.RelationalEntity;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.hook.PolicyHookDispatcher;
import org.apache.gravitino.hook.TagHookDispatcher;
import org.apache.gravitino.listener.api.event.AddPolicyForTagEvent;
import org.apache.gravitino.listener.api.event.AddPolicyForTagPreEvent;
import org.apache.gravitino.listener.api.event.RemovePolicyFromTagEvent;
import org.apache.gravitino.listener.api.event.RemovePolicyFromTagPreEvent;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.policy.AllValuesSelector;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.tag.TagDispatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyTagDispatcherChain {
  private static final String METALAKE = "metalake";
  private static final String TAG = "tag";
  private static final String POLICY = "policy";

  @Test
  void testTagDispatcherChainDelegatesPolicyTagLookups() {
    TagDispatcher delegate = mock(TagDispatcher.class);
    PolicyEntity policy = mock(PolicyEntity.class);
    when(policy.name()).thenReturn(POLICY);
    RelationalEntity<PolicyEntity> association =
        new RelationalEntity<>(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            NameIdentifier.of(METALAKE, TAG),
            Entity.EntityType.TAG,
            policy);
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new RelationalEntity<?>[] {association});

    TagDispatcher dispatcher =
        new TagHookDispatcher(
            new TagEventDispatcher(new EventBus(Collections.emptyList()), delegate));

    Assertions.assertArrayEquals(
        new String[] {POLICY}, dispatcher.listPoliciesForTag(METALAKE, TAG));
    Assertions.assertArrayEquals(
        new RelationalEntity<?>[] {association},
        dispatcher.listPolicyAssociationsForTag(METALAKE, TAG));
    verify(delegate, times(2)).listPolicyAssociationsForTag(METALAKE, TAG);
  }

  @Test
  void testTagDispatcherChainDelegatesMutationsAndEmitsEvents() {
    TagDispatcher delegate = mock(TagDispatcher.class);
    when(delegate.listPolicyAssociationsForTag(METALAKE, TAG))
        .thenReturn(new RelationalEntity<?>[0]);
    DummyEventListener listener = new DummyEventListener();
    TagDispatcher dispatcher =
        new TagHookDispatcher(
            new TagEventDispatcher(new EventBus(Collections.singletonList(listener)), delegate));

    dispatcher.addPolicyForTag(METALAKE, TAG, POLICY, AllValuesSelector.get());
    dispatcher.removePolicyFromTag(METALAKE, TAG, POLICY);

    verify(delegate).addPolicyForTag(METALAKE, TAG, POLICY, AllValuesSelector.get());
    verify(delegate).removePolicyFromTag(METALAKE, TAG, POLICY);
    verify(delegate, times(2)).listPolicyAssociationsForTag(METALAKE, TAG);
    Assertions.assertInstanceOf(AddPolicyForTagPreEvent.class, listener.getPreEvents().get(0));
    Assertions.assertInstanceOf(RemovePolicyFromTagPreEvent.class, listener.getPreEvents().get(1));
    Assertions.assertInstanceOf(AddPolicyForTagEvent.class, listener.getPostEvents().get(0));
    Assertions.assertInstanceOf(RemovePolicyFromTagEvent.class, listener.getPostEvents().get(1));
  }

  @Test
  void testPolicyDispatcherChainDelegatesPolicyTagLookups() {
    PolicyDispatcher delegate = mock(PolicyDispatcher.class);
    TagEntity tag = mock(TagEntity.class);
    when(tag.name()).thenReturn(TAG);
    RelationalEntity<TagEntity> association =
        new RelationalEntity<>(
            SupportsRelationOperations.Type.POLICY_TAG_REL,
            NameIdentifier.of(METALAKE, POLICY),
            Entity.EntityType.POLICY,
            tag);
    when(delegate.listTagAssociationsForPolicy(METALAKE, POLICY))
        .thenReturn(new RelationalEntity<?>[] {association});

    PolicyDispatcher dispatcher =
        new PolicyHookDispatcher(
            new PolicyEventDispatcher(new EventBus(Collections.emptyList()), delegate));

    Assertions.assertArrayEquals(
        new String[] {TAG}, dispatcher.listTagsForPolicy(METALAKE, POLICY));
    Assertions.assertArrayEquals(
        new RelationalEntity<?>[] {association},
        dispatcher.listTagAssociationsForPolicy(METALAKE, POLICY));
    verify(delegate, times(2)).listTagAssociationsForPolicy(METALAKE, POLICY);
  }
}
