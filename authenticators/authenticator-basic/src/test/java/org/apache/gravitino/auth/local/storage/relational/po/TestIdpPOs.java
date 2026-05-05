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
package org.apache.gravitino.auth.local.storage.relational.po;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class TestIdpPOs {

  @Test
  void testIdpUserPOBuilderAndEquality() {
    IdpUserPO userPO =
        IdpUserPO.builder()
            .withUserId(1L)
            .withUserName("alice")
            .withPasswordHash("hash")
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    IdpUserPO sameUserPO =
        IdpUserPO.builder()
            .withUserId(1L)
            .withUserName("alice")
            .withPasswordHash("hash")
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    assertEquals(userPO, sameUserPO);
    assertEquals(userPO.hashCode(), sameUserPO.hashCode());
    assertEquals("hash", userPO.getPasswordHash());
  }

  @Test
  void testIdpUserPOBuilderValidation() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IdpUserPO.builder()
                .withUserId(1L)
                .withUserName("alice")
                .withAuditInfo("audit")
                .withCurrentVersion(1L)
                .withLastVersion(1L)
                .withDeletedAt(0L)
                .build());
  }

  @Test
  void testIdpGroupPOBuilderAndEquality() {
    IdpGroupPO groupPO =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("engineering")
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    IdpGroupPO sameGroupPO =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("engineering")
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    assertEquals(groupPO, sameGroupPO);
    assertEquals(groupPO.hashCode(), sameGroupPO.hashCode());
    assertEquals("engineering", groupPO.getGroupName());
  }

  @Test
  void testIdpGroupPOBuilderValidation() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IdpGroupPO.builder()
                .withGroupId(1L)
                .withAuditInfo("audit")
                .withCurrentVersion(1L)
                .withLastVersion(1L)
                .withDeletedAt(0L)
                .build());
  }

  @Test
  void testIdpGroupUserRelPOBuilderAndEquality() {
    IdpGroupUserRelPO relPO =
        IdpGroupUserRelPO.builder()
            .withId(1L)
            .withGroupId(2L)
            .withUserId(3L)
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    IdpGroupUserRelPO sameRelPO =
        IdpGroupUserRelPO.builder()
            .withId(1L)
            .withGroupId(2L)
            .withUserId(3L)
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    IdpGroupUserRelPO differentRelPO =
        IdpGroupUserRelPO.builder()
            .withId(2L)
            .withGroupId(2L)
            .withUserId(3L)
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    assertEquals(relPO, sameRelPO);
    assertEquals(relPO.hashCode(), sameRelPO.hashCode());
    assertNotEquals(relPO, differentRelPO);
    assertEquals(1L, relPO.getId());
  }

  @Test
  void testIdpGroupUserRelPOBuilderValidation() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            IdpGroupUserRelPO.builder()
                .withGroupId(2L)
                .withUserId(3L)
                .withAuditInfo("audit")
                .withCurrentVersion(1L)
                .withLastVersion(1L)
                .withDeletedAt(0L)
                .build());
  }
}
