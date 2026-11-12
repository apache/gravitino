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
package org.apache.gravitino.domain;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NameValidationUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class TestDomainNormalizeDispatcher {
  private final DomainDispatcher delegate = mock(DomainDispatcher.class);
  private final DomainDispatcher dispatcher = new DomainNormalizeDispatcher(delegate);
  private final TagValue tag = TagValue.noValue("department");

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(
      strings = {
        " ",
        ".",
        "..",
        "-start",
        "a.b",
        "A/B",
        "a b",
        "财务",
        "*",
        "SyStEm",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      })
  void rejectsInvalidNames(String name) {
    assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.createDomain("m", name, null, null, tag, false));
    assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.createSubdomain("m", "parent", name, null, null, tag, false));
    assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.alterDomain("m", "old", DomainChange.rename(name)));
    assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.createSubdomain("m", name, "child", null, null, tag, false));
    assertThrows(IllegalArgumentException.class, () -> dispatcher.getDomain("m", name));
    assertThrows(IllegalArgumentException.class, () -> dispatcher.deleteDomain("m", name));
    assertThrows(IllegalArgumentException.class, () -> dispatcher.attachDomain("m", "child", name));
    assertThrows(IllegalArgumentException.class, () -> dispatcher.alterDomain("m", name));
    assertThrows(IllegalArgumentException.class, () -> dispatcher.publishDomain("m", name));
    assertThrows(IllegalArgumentException.class, () -> dispatcher.unpublishDomain("m", name));
    assertThrows(IllegalArgumentException.class, () -> dispatcher.updateBackingTag("m", name, tag));
    assertThrows(IllegalArgumentException.class, () -> dispatcher.listSubdomainInfos("m", name));
    assertThrows(
        IllegalArgumentException.class, () -> dispatcher.attachDomain("m", name, "parent"));
    assertThrows(IllegalArgumentException.class, () -> dispatcher.detachDomain("m", name));
    assertThrows(
        IllegalArgumentException.class,
        () -> dispatcher.listMembers("m", name, DomainMemberFilter.builder().build()));
    verifyNoInteractions(delegate);
  }

  @Test
  void rejectsNullNameAndNullScopeCollection() {
    assertEquals(
        "The domain name must not be null.",
        assertThrows(
                IllegalArgumentException.class,
                () -> NameValidationUtils.validateName("domain", null))
            .getMessage());
    assertEquals(
        "Domain names must not be null.",
        assertThrows(
                IllegalArgumentException.class, () -> NameValidationUtils.validateDomainNames(null))
            .getMessage());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "0",
        "_",
        "Finance-Trading_APAC",
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      })
  void preservesValidNames(String name) {
    dispatcher.createDomain("m", name, null, null, tag, false);
    verify(delegate).createDomain("m", name, null, null, tag, false);
    dispatcher.createSubdomain("m", "parent", name, null, null, tag, false);
    verify(delegate).createSubdomain("m", "parent", name, null, null, tag, false);
    DomainChange rename = DomainChange.rename(name);
    dispatcher.alterDomain("m", "old", rename);
    verify(delegate).alterDomain("m", "old", rename);
    dispatcher.deleteDomain("m", "parent");
    verify(delegate).deleteDomain("m", "parent");
  }

  @Test
  void rejectsInvalidMemberScopesBeforeDispatch() {
    DomainEntity domain = mock(DomainEntity.class);
    DomainEntity[] children = new DomainEntity[0];
    for (String[] names : new String[][] {{"bad.name"}, {"Finance", "bad.name"}}) {
      DomainMemberFilter filter = DomainMemberFilter.builder().withDomains(names).build();
      assertThrows(
          IllegalArgumentException.class, () -> dispatcher.listMembers("m", "Finance", filter));
      assertThrows(
          IllegalArgumentException.class,
          () -> dispatcher.listMembers("m", domain, children, filter));
    }
    verifyNoInteractions(delegate);
  }

  @Test
  void forwardsValidMemberScopesAndSearchText() {
    DomainEntity domain = mock(DomainEntity.class);
    DomainEntity[] children = new DomainEntity[0];
    DomainMemberFilter filter =
        DomainMemberFilter.builder()
            .withDomains("Finance", "Unknown")
            .withNameContains("a.b/c")
            .build();
    dispatcher.listMembers("m", "Finance", filter);
    verify(delegate).listMembers("m", "Finance", filter);
    dispatcher.listMembers("m", domain, children, filter);
    verify(delegate).listMembers("m", domain, children, filter);
  }

  @Test
  void preservesValidationWithMutationCheck() {
    Consumer<LockedDomainState> check = state -> {};
    DomainDispatcher checked = mock(DomainDispatcher.class);
    when(delegate.withMutationCheck(check)).thenReturn(checked);
    DomainDispatcher scoped = dispatcher.withMutationCheck(check);
    assertThrows(
        IllegalArgumentException.class,
        () -> scoped.createDomain("m", ".", null, null, tag, false));
    verifyNoInteractions(checked);
    scoped.createDomain("m", "finance", null, null, tag, false);
    verify(checked).createDomain("m", "finance", null, null, tag, false);
  }
}
