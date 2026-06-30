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

package org.apache.gravitino.idp.storage.mapper.provider.base;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class TestIdpSoftDeleteSQLProvider {

  private static final String MILLISECOND_TIMESTAMP_COMPONENT =
      "EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000";

  @Test
  void testUserDeletedAtPrecision() {
    String sql = new IdpUserMetaBaseSQLProvider().softDeleteIdpUser("user1");

    assertUsesMillisecondTimestamp(sql);
  }

  @Test
  void testGroupDeletedAtPrecision() {
    String sql = new IdpGroupMetaBaseSQLProvider().softDeleteIdpGroup("group1");

    assertUsesMillisecondTimestamp(sql);
  }

  @Test
  void testRelationDeletedAtPrecision() {
    IdpUserGroupRelBaseSQLProvider provider = new IdpUserGroupRelBaseSQLProvider();

    assertUsesMillisecondTimestamp(provider.softDeleteRelations("group1", List.of("user1")));
    assertUsesMillisecondTimestamp(provider.softDeleteRelationsByUsername("user1"));
    assertUsesMillisecondTimestamp(provider.softDeleteRelationsByGroupName("group1"));
  }

  private static void assertUsesMillisecondTimestamp(String sql) {
    assertTrue(sql.contains("(UNIX_TIMESTAMP() * 1000.0)"), sql);
    assertTrue(sql.contains(MILLISECOND_TIMESTAMP_COMPONENT), sql);
  }
}
