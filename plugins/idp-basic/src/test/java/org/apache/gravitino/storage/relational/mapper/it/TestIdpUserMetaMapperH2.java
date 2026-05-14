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

package org.apache.gravitino.storage.relational.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.storage.relational.mapper.it.BackendTypes;
import org.junit.jupiter.api.TestTemplate;

@BackendTypes({"h2"})
public class TestIdpUserMetaMapperH2 extends IdpMapperTestBase implements IdpUserMetaMapperTest {

  @TestTemplate
  public void testInsertIdpUserAndSelectIdpUser() {
    IdpUserMetaMapperTest.super.testInsertIdpUserAndSelectIdpUser();
  }

  @TestTemplate
  public void testSelectIdpUsers() {
    IdpUserMetaMapperTest.super.testSelectIdpUsers();
  }

  @TestTemplate
  public void testSelectIdpUsersIgnoresDeletedUsers() {
    IdpUserMetaMapperTest.super.testSelectIdpUsersIgnoresDeletedUsers();
  }

  @TestTemplate
  public void testUpdateIdpUserPassword() {
    IdpUserMetaMapperTest.super.testUpdateIdpUserPassword();
  }

  @TestTemplate
  public void testUpdateIdpUserPasswordKeepsVersionsUnchanged() {
    IdpUserMetaMapperTest.super.testUpdateIdpUserPasswordKeepsVersionsUnchanged();
  }

  @TestTemplate
  public void testUpdateIdpUserPasswordReturnsZeroForDeletedUser() {
    IdpUserMetaMapperTest.super.testUpdateIdpUserPasswordReturnsZeroForDeletedUser();
  }

  @TestTemplate
  public void testSoftDeleteIdpUser() {
    IdpUserMetaMapperTest.super.testSoftDeleteIdpUser();
  }

  @TestTemplate
  public void testSoftDeleteIdpUserReturnsZeroForDeletedUser() {
    IdpUserMetaMapperTest.super.testSoftDeleteIdpUserReturnsZeroForDeletedUser();
  }

  @TestTemplate
  public void testSoftDeleteIdpUserRunsWithH2MysqlMode() {
    assertEquals("h2", backendType);
    assertTrue(currentJdbcUrl().contains("MODE=MYSQL"));

    insertUser(1L, "alice", "hash-a", 1L, 0L, 0L);
    assertEquals(1, idpUserMetaMapper.softDeleteIdpUser(1L));
    assertTrue(queryLongValueInMapperTest("idp_user_meta", "deleted_at", "user_id", 1L) > 0L);
  }

  @TestTemplate
  public void testDeleteIdpUserMetasByLegacyTimeline() {
    IdpUserMetaMapperTest.super.testDeleteIdpUserMetasByLegacyTimeline();
  }
}
