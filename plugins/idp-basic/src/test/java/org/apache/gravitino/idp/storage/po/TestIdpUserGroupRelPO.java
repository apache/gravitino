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
package org.apache.gravitino.idp.storage.po;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpUserGroupRelPO {

  @Test
  public void testIdpUserGroupRelPOBuilder() {
    IdpUserGroupRelPO relPO =
        IdpUserGroupRelPO.builder()
            .withId(1L)
            .withGroupId(10L)
            .withUserId(20L)
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();

    Assertions.assertEquals(1L, relPO.getId());
    Assertions.assertEquals(10L, relPO.getGroupId());
    Assertions.assertEquals(20L, relPO.getUserId());
    Assertions.assertEquals(1L, relPO.getCurrentVersion());
    Assertions.assertEquals(0L, relPO.getLastVersion());
    Assertions.assertEquals(0L, relPO.getDeletedAt());
  }

  @Test
  public void testEqualsAndHashCode() {
    IdpUserGroupRelPO relPO1 =
        IdpUserGroupRelPO.builder()
            .withId(1L)
            .withGroupId(10L)
            .withUserId(20L)
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();

    IdpUserGroupRelPO relPO2 =
        IdpUserGroupRelPO.builder()
            .withId(1L)
            .withGroupId(10L)
            .withUserId(20L)
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();

    Assertions.assertEquals(relPO1, relPO2);
    Assertions.assertEquals(relPO1.hashCode(), relPO2.hashCode());
  }

  @Test
  public void testBuilderReuseDoesNotMutateBuiltObject() {
    var builder =
        IdpUserGroupRelPO.builder()
            .withId(1L)
            .withGroupId(10L)
            .withUserId(20L)
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L);

    IdpUserGroupRelPO firstRelation = builder.build();
    IdpUserGroupRelPO secondRelation = builder.withUserId(21L).build();

    Assertions.assertEquals(20L, firstRelation.getUserId());
    Assertions.assertEquals(21L, secondRelation.getUserId());
  }
}
