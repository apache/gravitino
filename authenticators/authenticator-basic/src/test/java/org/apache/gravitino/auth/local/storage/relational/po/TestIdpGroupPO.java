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

public class TestIdpGroupPO {

  @Test
  public void testIdpGroupPOBuilder() {
    IdpGroupPO groupPO =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("engineering")
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    Assertions.assertEquals(1L, groupPO.getGroupId());
    Assertions.assertEquals("engineering", groupPO.getGroupName());
    Assertions.assertEquals("audit", groupPO.getAuditInfo());
    Assertions.assertEquals(1L, groupPO.getCurrentVersion());
    Assertions.assertEquals(1L, groupPO.getLastVersion());
    Assertions.assertEquals(0L, groupPO.getDeletedAt());
  }

  @Test
  public void testIdpGroupPOBuilderValidation() {
    Assertions.assertThrows(
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
  public void testEqualsAndHashCode() {
    IdpGroupPO groupPO1 =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("engineering")
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    IdpGroupPO groupPO2 =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupName("engineering")
            .withAuditInfo("audit")
            .withCurrentVersion(1L)
            .withLastVersion(1L)
            .withDeletedAt(0L)
            .build();

    Assertions.assertEquals(groupPO1, groupPO2);
    Assertions.assertEquals(groupPO1.hashCode(), groupPO2.hashCode());
  }
}
