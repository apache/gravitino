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
package org.apache.gravitino.hook;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class TestDomainHookDispatcher {

  @Test
  void creatorsOwnDomainsAndOnlyNewBackingTags() {
    GravitinoEnv env = mock(GravitinoEnv.class);
    OwnerDispatcher owners = mock(OwnerDispatcher.class);
    when(env.internalOwnerDispatcher()).thenReturn(owners);
    DomainDispatcher internal = mock(DomainDispatcher.class);
    DomainEntity domain = domain("Finance");
    when(internal.createDomain(anyString(), anyString(), any(), any(), any(), eq(true)))
        .thenReturn(domain);
    when(internal.createSubdomain(
            anyString(), anyString(), anyString(), any(), any(), any(), eq(false)))
        .thenReturn(domain("Trading"));
    try (MockedStatic<GravitinoEnv> environment = mockStatic(GravitinoEnv.class)) {
      environment.when(GravitinoEnv::getInstance).thenReturn(env);
      DomainHookDispatcher dispatcher = new DomainHookDispatcher(internal);
      dispatcher.createDomain("lake", "Finance", null, null, TagValue.noValue("newTag"), true);
      verify(owners)
          .setOwner(
              "lake",
              MetadataObjects.of(null, "Finance", MetadataObject.Type.DOMAIN),
              PrincipalUtils.getCurrentUserName(),
              Owner.Type.USER);
      verify(owners)
          .setOwner(
              "lake",
              MetadataObjects.of(null, "newTag", MetadataObject.Type.TAG),
              PrincipalUtils.getCurrentUserName(),
              Owner.Type.USER);
      dispatcher.createSubdomain(
          "lake", "Finance", "Trading", null, null, TagValue.noValue("existingTag"), false);
      verify(owners)
          .setOwner(
              "lake",
              MetadataObjects.of(null, "Trading", MetadataObject.Type.DOMAIN),
              PrincipalUtils.getCurrentUserName(),
              Owner.Type.USER);
      verify(owners, never())
          .setOwner(
              eq("lake"),
              eq(MetadataObjects.of(null, "existingTag", MetadataObject.Type.TAG)),
              anyString(),
              any());
    }
  }

  @Test
  void renameAndDeleteInvalidateOldNameMapping() {
    GravitinoEnv env = mock(GravitinoEnv.class);
    GravitinoAuthorizer authorizer = mock(GravitinoAuthorizer.class);
    when(env.gravitinoAuthorizer()).thenReturn(authorizer);
    DomainDispatcher internal = mock(DomainDispatcher.class);
    when(internal.alterDomain(eq("lake"), eq("Finance"), any())).thenReturn(domain("Markets"));
    when(internal.deleteDomain("lake", "Markets")).thenReturn(true);
    try (MockedStatic<GravitinoEnv> environment = mockStatic(GravitinoEnv.class)) {
      environment.when(GravitinoEnv::getInstance).thenReturn(env);
      DomainHookDispatcher dispatcher = new DomainHookDispatcher(internal);
      dispatcher.alterDomain("lake", "Finance", DomainChange.rename("Markets"));
      verify(authorizer)
          .handleEntityNameIdMappingChange(
              "lake", NameIdentifierUtil.ofDomain("lake", "Finance"), Entity.EntityType.DOMAIN);
      assertTrue(dispatcher.deleteDomain("lake", "Markets"));
      verify(authorizer)
          .handleEntityNameIdMappingChange(
              "lake", NameIdentifierUtil.ofDomain("lake", "Markets"), Entity.EntityType.DOMAIN);
      dispatcher.deleteDomain("lake", "missing");
      verify(authorizer, never())
          .handleEntityNameIdMappingChange(
              "lake", NameIdentifierUtil.ofDomain("lake", "missing"), Entity.EntityType.DOMAIN);
    }
  }

  private static DomainEntity domain(String name) {
    return DomainEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(NamespaceUtil.ofDomain("lake"))
        .withStatus(Domain.Status.DRAFT)
        .withProperties(Collections.emptyMap())
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.EPOCH).build())
        .build();
  }
}
