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

package org.apache.gravitino.idp.storage.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import org.apache.gravitino.idp.storage.po.IdpGroupPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("gravitino-docker-test")
class TestIdpGroupMetaStorage extends AbstractIdpMetaStorageTest {

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testInsertIdpGroupAndSelectIdpGroup(String type) throws IOException {
    init(type);
    IdpGroupPO firstGroup =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupname("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    idpGroupMetaMapper.insertIdpGroup(firstGroup);

    assertEquals(firstGroup, idpGroupMetaMapper.selectIdpGroup("dev"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("unknown"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSelectIdpGroupIgnoresDeletedGroups(String type) throws IOException {
    init(type);
    IdpGroupPO activeGroup =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupname("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build();
    idpGroupMetaMapper.insertIdpGroup(activeGroup);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(2L)
            .withGroupname("ops")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(10L)
            .build());

    assertEquals(activeGroup, idpGroupMetaMapper.selectIdpGroup("dev"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("ops"));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testSoftDeleteIdpGroup(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupname("dev")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());

    assertEquals(1, idpGroupMetaMapper.softDeleteIdpGroup(1L));
    assertNull(idpGroupMetaMapper.selectIdpGroup("dev"));
    assertTrue(queryLongValue("idp_group_meta", "deleted_at", "group_id", 1L) > 0L);
    assertEquals(1L, queryLongValue("idp_group_meta", "current_version", "group_id", 1L));
    assertEquals(0L, queryLongValue("idp_group_meta", "last_version", "group_id", 1L));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteIdpGroupMetasByLegacyTimeline(String type) throws IOException {
    init(type);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupname("legacy-group")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(10L)
            .build());
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(2L)
            .withGroupname("new-group")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(30L)
            .build());
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(3L)
            .withGroupname("active-group")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(0L)
            .build());

    assertEquals(1, idpGroupMetaMapper.deleteIdpGroupMetasByLegacyTimeline(20L, 10));
    assertEquals(0, countRows("idp_group_meta", "group_id", 1L));
    assertEquals(1, countRows("idp_group_meta", "group_id", 2L));
    assertEquals(1, countRows("idp_group_meta", "group_id", 3L));
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testRestart(String type) throws IOException {
    init(type);
    IdpGroupPO expectedActiveGroup =
        IdpGroupPO.builder()
            .withGroupId(1L)
            .withGroupname("dev")
            .withCurrentVersion(3L)
            .withLastVersion(2L)
            .withDeletedAt(0L)
            .build();
    idpGroupMetaMapper.insertIdpGroup(expectedActiveGroup);
    idpGroupMetaMapper.insertIdpGroup(
        IdpGroupPO.builder()
            .withGroupId(2L)
            .withGroupname("ops")
            .withCurrentVersion(1L)
            .withLastVersion(0L)
            .withDeletedAt(10L)
            .build());

    assertPersistedGroups(expectedActiveGroup);

    restartBackend();

    assertPersistedGroups(expectedActiveGroup);
  }

  private void assertPersistedGroups(IdpGroupPO expectedActiveGroup) {
    assertTrue(
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .hasMapper(IdpGroupMetaMapper.class));

    assertEquals(expectedActiveGroup, idpGroupMetaMapper.selectIdpGroup("dev"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("ops"));
  }
}
