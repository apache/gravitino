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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestTopicMetaBaseSQLProvider {

  private static final TopicMetaBaseSQLProvider PROVIDER = new TopicMetaBaseSQLProvider();

  @Test
  void testRestoreOnlyUpdatesSoftDeletedStableId() {
    String sql = PROVIDER.restoreSoftDeletedTopicMeta(null);

    Assertions.assertTrue(sql.endsWith("WHERE topic_id = #{topicMeta.topicId} AND deleted_at > 0"));
    Assertions.assertTrue(sql.contains("last_version = current_version + 1"));
    Assertions.assertTrue(sql.contains("current_version = current_version + 1"));
  }
}
