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
package org.apache.gravitino.server.authorization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.domain.DomainMemberInfo;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

class TestDomainMembershipAuthorization {

  private static final MetadataObject TABLE =
      MetadataObjects.parse("cat.db.trades", MetadataObject.Type.TABLE);
  private final DomainDispatcher dispatcher = mock(DomainDispatcher.class);
  private final GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
  private final DomainEntity parent = domain("Finance", null, Domain.Status.PUBLISHED);
  private final DomainEntity hidden = domain("Trading", "Finance", Domain.Status.DRAFT);
  private MockedStatic<GravitinoEnv> environment;

  @BeforeEach
  void setUp() {
    GravitinoEnv env = mock(GravitinoEnv.class);
    Config config = mock(Config.class);
    when(config.get(Configs.ENABLE_AUTHORIZATION)).thenReturn(true);
    when(config.get(Configs.GRAVITINO_AUTHORIZATION_THREAD_POOL_SIZE)).thenReturn(2);
    when(env.config()).thenReturn(config);
    when(env.internalDomainDispatcher()).thenReturn(dispatcher);
    when(env.gravitinoAuthorizer()).thenReturn(authorizer);
    environment = mockStatic(GravitinoEnv.class);
    environment.when(GravitinoEnv::getInstance).thenReturn(env);
    when(dispatcher.getDomain("lake", "Finance")).thenReturn(parent);
    when(dispatcher.listSubdomainInfos("lake", "Finance")).thenReturn(new DomainEntity[] {hidden});
    when(authorizer.authorize(any(), anyString(), any(), any(), any()))
        .thenAnswer(
            call -> {
              Privilege.Name privilege = call.getArgument(3);
              return privilege == Privilege.Name.USE_DOMAIN
                  || privilege == Privilege.Name.USE_CATALOG
                  || privilege == Privilege.Name.USE_SCHEMA
                  || privilege == Privilege.Name.SELECT_TABLE;
            });
  }

  @AfterEach
  void tearDown() {
    environment.close();
  }

  @Test
  void hiddenChildMembersAndRelationsAreRemovedFromUnfilteredResults() {
    DomainMember both =
        new DomainMemberInfo(
            TABLE,
            new DomainRelation[] {DomainRelation.direct(), DomainRelation.viaSubdomain("Trading")});
    DomainMember hiddenOnly =
        new DomainMemberInfo(TABLE, new DomainRelation[] {DomainRelation.viaSubdomain("Trading")});
    when(dispatcher.listMembers(eq("lake"), eq(parent), any(DomainEntity[].class), any()))
        .thenReturn(new DomainMember[] {both, hiddenOnly});
    DomainMember[] members =
        DomainAuthzHelper.listMembers(
            dispatcher, "lake", "Finance", DomainMemberFilter.builder().build());
    assertEquals(1, members.length);
    assertArrayEquals(new DomainRelation[] {DomainRelation.direct()}, members[0].relations());
  }

  @Test
  void hiddenOrUnknownSelectionsCannotWidenResults() {
    when(dispatcher.listMembers(eq("lake"), eq(parent), any(DomainEntity[].class), any()))
        .thenReturn(new DomainMember[0]);
    for (String selected : new String[] {"Trading", "unknown"}) {
      DomainMemberFilter filter = DomainMemberFilter.builder().withDomains(selected).build();
      assertEquals(0, DomainAuthzHelper.listMembers(dispatcher, "lake", "Finance", filter).length);
      verify(dispatcher)
          .listMembers(
              eq("lake"), eq(parent), argThat(snapshots -> snapshots.length == 0), eq(filter));
    }
    verify(dispatcher, never()).listMembers(anyString(), anyString(), any());
  }

  @Test
  void mixedSelectionsKeepOnlyVisibleScopesAndPreserveOtherFilters() {
    DomainEntity visible = domain("Tax", "Finance", Domain.Status.PUBLISHED);
    when(dispatcher.listSubdomainInfos("lake", "Finance"))
        .thenReturn(new DomainEntity[] {hidden, visible});
    when(dispatcher.listMembers(eq("lake"), eq(parent), any(DomainEntity[].class), any()))
        .thenReturn(new DomainMember[0]);
    DomainAuthzHelper.listMembers(
        dispatcher,
        "lake",
        "Finance",
        DomainMemberFilter.builder()
            .withDomains("Trading", "Tax", "unknown")
            .withTypes(MetadataObject.Type.TABLE)
            .withNameContains("trades")
            .build());
    ArgumentCaptor<DomainMemberFilter> filter = ArgumentCaptor.forClass(DomainMemberFilter.class);
    verify(dispatcher)
        .listMembers(eq("lake"), eq(parent), any(DomainEntity[].class), filter.capture());
    assertEquals(Collections.singleton("Tax"), filter.getValue().domains());
    assertEquals(Collections.singleton(MetadataObject.Type.TABLE), filter.getValue().types());
    assertEquals("trades", filter.getValue().nameContains().get());
  }

  @Test
  void expandSurvivesVisibleScopeSelection() {
    DomainEntity visible = domain("Tax", "Finance", Domain.Status.PUBLISHED);
    when(dispatcher.listSubdomainInfos("lake", "Finance"))
        .thenReturn(new DomainEntity[] {hidden, visible});
    when(dispatcher.listMembers(eq("lake"), eq(parent), any(DomainEntity[].class), any()))
        .thenReturn(new DomainMember[0]);
    DomainAuthzHelper.listMembers(
        dispatcher,
        "lake",
        "Finance",
        DomainMemberFilter.builder().withDomains("Trading", "Tax").withExpand(true).build());
    ArgumentCaptor<DomainMemberFilter> filter = ArgumentCaptor.forClass(DomainMemberFilter.class);
    verify(dispatcher)
        .listMembers(eq("lake"), eq(parent), any(DomainEntity[].class), filter.capture());
    assertEquals(
        DomainMemberFilter.builder().withDomains("Tax").withExpand(true).build(),
        filter.getValue());
  }

  @Test
  void catalogAndSchemaMembersAreFilteredByObjectAccess() {
    MetadataObject catalog = MetadataObjects.parse("cat", MetadataObject.Type.CATALOG);
    MetadataObject schema = MetadataObjects.parse("cat.db", MetadataObject.Type.SCHEMA);
    MetadataObject secretCatalog = MetadataObjects.parse("secret", MetadataObject.Type.CATALOG);
    MetadataObject secretSchema = MetadataObjects.parse("cat.hidden", MetadataObject.Type.SCHEMA);
    DomainRelation[] direct = {DomainRelation.direct()};
    when(dispatcher.listMembers(eq("lake"), eq(parent), any(DomainEntity[].class), any()))
        .thenReturn(
            new DomainMember[] {
              new DomainMemberInfo(catalog, direct),
              new DomainMemberInfo(schema, direct),
              new DomainMemberInfo(secretCatalog, direct),
              new DomainMemberInfo(secretSchema, direct)
            });
    when(authorizer.deny(
            any(), anyString(), eq(secretCatalog), eq(Privilege.Name.USE_CATALOG), any()))
        .thenReturn(true);
    when(authorizer.deny(
            any(), anyString(), eq(secretSchema), eq(Privilege.Name.USE_SCHEMA), any()))
        .thenReturn(true);
    DomainMember[] members =
        DomainAuthzHelper.listMembers(
            dispatcher, "lake", "Finance", DomainMemberFilter.builder().build());
    assertArrayEquals(
        new MetadataObject[] {catalog, schema},
        Arrays.stream(members).map(DomainMember::object).toArray(MetadataObject[]::new));
  }

  @Test
  void objectAccessIsCheckedWithoutTagPrivileges() {
    when(dispatcher.listMembers(eq("lake"), eq(parent), any(DomainEntity[].class), any()))
        .thenReturn(
            new DomainMember[] {
              new DomainMemberInfo(TABLE, new DomainRelation[] {DomainRelation.direct()})
            });
    when(authorizer.deny(any(), anyString(), eq(TABLE), eq(Privilege.Name.SELECT_TABLE), any()))
        .thenReturn(true);
    assertEquals(
        0,
        DomainAuthzHelper.listMembers(
                dispatcher, "lake", "Finance", DomainMemberFilter.builder().build())
            .length);
    when(authorizer.deny(any(), anyString(), eq(TABLE), eq(Privilege.Name.SELECT_TABLE), any()))
        .thenReturn(false);
    assertEquals(
        1,
        DomainAuthzHelper.listMembers(
                dispatcher, "lake", "Finance", DomainMemberFilter.builder().build())
            .length);
    verify(authorizer, never())
        .authorize(any(), anyString(), any(), eq(Privilege.Name.VIEW_TAG), any());
    verify(authorizer, never())
        .authorize(any(), anyString(), any(), eq(Privilege.Name.APPLY_TAG), any());
  }

  @Test
  void membershipResolutionUsesTheAuthorizedSnapshotWhenNameIsReused() {
    DomainEntity replacement =
        parent.toBuilder().withId(2L).withStatus(Domain.Status.DRAFT).build();
    when(dispatcher.getDomain("lake", "Finance")).thenReturn(parent, replacement);
    when(dispatcher.listMembers(eq("lake"), eq(parent), any(DomainEntity[].class), any()))
        .thenReturn(
            new DomainMember[] {
              new DomainMemberInfo(TABLE, new DomainRelation[] {DomainRelation.direct()})
            });

    DomainMember[] members =
        DomainAuthzHelper.listMembers(
            dispatcher, "lake", "Finance", DomainMemberFilter.builder().build());

    assertEquals(1, members.length);
    verify(dispatcher).getDomain("lake", "Finance");
    verify(dispatcher, never()).listMembers(anyString(), anyString(), any());
    verify(dispatcher).listMembers(eq("lake"), eq(parent), any(DomainEntity[].class), any());
    assertFalse(DomainAuthzHelper.visible("lake", replacement));
  }

  @Test
  void reverseLookupDropsHiddenChildAndItsParentRollup() {
    DomainEntity parentRollup =
        parent.toBuilder()
            .withRelations(new DomainRelation[] {DomainRelation.viaSubdomain("Trading")})
            .build();
    DomainEntity childDirect =
        hidden.toBuilder().withRelations(new DomainRelation[] {DomainRelation.direct()}).build();
    when(dispatcher.listDomainsForMetadataObject("lake", TABLE))
        .thenReturn(new DomainEntity[] {parentRollup, childDirect});
    assertEquals(
        0, DomainAuthzHelper.listDomainsForMetadataObject(dispatcher, "lake", TABLE).length);
    DomainEntity parentDirect =
        parentRollup.toBuilder()
            .withRelations(
                new DomainRelation[] {
                  DomainRelation.direct(), DomainRelation.viaSubdomain("Trading")
                })
            .build();
    when(dispatcher.listDomainsForMetadataObject("lake", TABLE))
        .thenReturn(new DomainEntity[] {parentDirect, childDirect});
    DomainEntity[] result =
        DomainAuthzHelper.listDomainsForMetadataObject(dispatcher, "lake", TABLE);
    assertEquals(1, result.length);
    assertArrayEquals(new DomainRelation[] {DomainRelation.direct()}, result[0].relations());
  }

  private static DomainEntity domain(String name, String parent, Domain.Status status) {
    return DomainEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(NamespaceUtil.ofDomain("lake"))
        .withParent(parent)
        .withStatus(status)
        .withProperties(Collections.emptyMap())
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
        .build();
  }
}
