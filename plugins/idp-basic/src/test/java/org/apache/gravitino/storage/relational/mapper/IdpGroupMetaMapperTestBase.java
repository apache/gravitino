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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.storage.relational.po.IdpGroupPO;
import org.junit.jupiter.api.Test;

abstract class IdpGroupMetaMapperTestBase extends IdpMapperTestBase {

  @Test
  void testInsertIdpGroupAndSelectIdpGroup() {
    IdpGroupPO group = insertGroup(10L, "dev", 1L, 0L, 0L);

    assertEquals(group, idpGroupMetaMapper.selectIdpGroup("dev"));
    assertNull(idpGroupMetaMapper.selectIdpGroup("unknown"));
  }

  @Test
  void testSoftDeleteIdpGroup() {
    insertGroup(10L, "dev", 1L, 0L, 0L);

    idpGroupMetaMapper.softDeleteIdpGroup(10L);
    assertNull(idpGroupMetaMapper.selectIdpGroup("dev"));
    assertTrue(queryLongValue("idp_group_meta", "deleted_at", "group_id", 10L) > 0L);
    assertEquals(2L, queryLongValue("idp_group_meta", "current_version", "group_id", 10L));
    assertEquals(1L, queryLongValue("idp_group_meta", "last_version", "group_id", 10L));
  }

  @Test
  void testDeleteIdpGroupMetasByLegacyTimeline() {
    insertGroup(10L, "legacy-group", 1L, 0L, 10L);
    insertGroup(20L, "new-group", 1L, 0L, 30L);
    insertGroup(30L, "active-group", 1L, 0L, 0L);

    assertEquals(1, idpGroupMetaMapper.deleteIdpGroupMetasByLegacyTimeline(20L, 10));

    assertEquals(0, countRows("idp_group_meta", "group_id", 10L));
    assertEquals(1, countRows("idp_group_meta", "group_id", 20L));
    assertEquals(1, countRows("idp_group_meta", "group_id", 30L));
  }
}
