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
package org.apache.gravitino.storage.relational.po;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpGroupUserRelPO {

  @Test
  public void testIdpGroupUserRelPOBuilder() {
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

    Assertions.assertEquals(1L, relPO.getId());
    Assertions.assertEquals(2L, relPO.getGroupId());
    Assertions.assertEquals(3L, relPO.getUserId());
    Assertions.assertEquals("audit", relPO.getAuditInfo());
    Assertions.assertEquals(1L, relPO.getCurrentVersion());
    Assertions.assertEquals(1L, relPO.getLastVersion());
    Assertions.assertEquals(0L, relPO.getDeletedAt());
  }

  @Test
  public void testIdpGroupUserRelPOBuilderValidation() {
    Assertions.assertThrows(
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

  @Test
  public void testEqualsAndHashCode() {
    IdpGroupUserRelPO relPO1 =
        IdpGroupUserRelPO.builder()
            .withId(1L)
            .withGroupId(2L)
            .withUserId(3L)
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    IdpGroupUserRelPO relPO2 =
        IdpGroupUserRelPO.builder()
            .withId(1L)
            .withGroupId(2L)
            .withUserId(3L)
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    Assertions.assertEquals(relPO1, relPO2);
    Assertions.assertEquals(relPO1.hashCode(), relPO2.hashCode());
  }
}
