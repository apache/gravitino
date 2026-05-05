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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpUserPO {

  @Test
  public void testIdpUserPOBuilder() {
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

    Assertions.assertEquals(1L, userPO.getUserId());
    Assertions.assertEquals("alice", userPO.getUserName());
    Assertions.assertEquals("hash", userPO.getPasswordHash());
    Assertions.assertEquals("audit", userPO.getAuditInfo());
    Assertions.assertEquals(1L, userPO.getCurrentVersion());
    Assertions.assertEquals(1L, userPO.getLastVersion());
    Assertions.assertEquals(0L, userPO.getDeletedAt());
  }

  @Test
  public void testIdpUserPOBuilderValidation() {
    Assertions.assertThrows(
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
  public void testEqualsAndHashCode() {
    IdpUserPO userPO1 =
        IdpUserPO.builder()
            .withUserId(1L)
            .withUserName("alice")
            .withPasswordHash("hash")
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    IdpUserPO userPO2 =
        IdpUserPO.builder()
            .withUserId(1L)
            .withUserName("alice")
            .withPasswordHash("hash")
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    Assertions.assertEquals(userPO1, userPO2);
    Assertions.assertEquals(userPO1.hashCode(), userPO2.hashCode());
  }
}
