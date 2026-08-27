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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import org.apache.gravitino.storage.relational.mapper.TopicMetaMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestTopicMetaPostgreSQLProvider {

  private static final TopicMetaPostgreSQLProvider PROVIDER = new TopicMetaPostgreSQLProvider();

  @Test
  void testOverwriteAdvancesStoredVersion() {
    String sql = PROVIDER.insertTopicMetaOnDuplicateKeyUpdate(null);
    String conflictClause = sql.substring(sql.indexOf(" ON CONFLICT"));

    Assertions.assertTrue(
        conflictClause.startsWith(" ON CONFLICT (schema_id, topic_name, deleted_at)"));
    Assertions.assertTrue(
        conflictClause.contains(
            "current_version = " + TopicMetaMapper.TABLE_NAME + ".current_version + 1"));
    Assertions.assertTrue(
        conflictClause.contains(
            "last_version = " + TopicMetaMapper.TABLE_NAME + ".current_version + 1"));
    Assertions.assertFalse(conflictClause.contains("#{topicMeta.currentVersion}"));
    Assertions.assertFalse(conflictClause.contains("#{topicMeta.lastVersion}"));
  }

  @Test
  void testUpdateUsesVersionCas() {
    String sql = PROVIDER.updateTopicMeta(null, null);
    String whereClause = sql.substring(sql.indexOf(" WHERE"));

    Assertions.assertEquals(
        " WHERE topic_id = #{oldTopicMeta.topicId}"
            + " AND current_version = #{oldTopicMeta.currentVersion}"
            + " AND deleted_at = 0",
        whereClause);
  }

  @Test
  void testDirectDeleteUsesVersionCas() {
    String sql = PROVIDER.softDeleteTopicMetasByTopicId(null, null);

    Assertions.assertTrue(sql.contains("AND current_version = #{currentVersion}"));
    Assertions.assertTrue(sql.endsWith("AND deleted_at = 0"));
  }
}
