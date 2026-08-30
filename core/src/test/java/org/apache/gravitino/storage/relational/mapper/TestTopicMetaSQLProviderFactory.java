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

import org.apache.gravitino.storage.relational.mapper.provider.base.TopicMetaBaseSQLProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestTopicMetaSQLProviderFactory {

  private static final TopicMetaBaseSQLProvider H2_PROVIDER =
      new TopicMetaSQLProviderFactory.TopicMetaH2Provider();

  @Test
  void testH2OverwriteCalculatesBothVersionsFromStoredRow() {
    String sql = H2_PROVIDER.insertTopicMetaOnDuplicateKeyUpdate(null);
    String updateClause = sql.substring(sql.indexOf(" ON DUPLICATE KEY UPDATE"));
    String nextVersionExpression = "GREATEST(current_version, last_version) + 1";

    Assertions.assertTrue(updateClause.contains("current_version = " + nextVersionExpression));
    Assertions.assertTrue(updateClause.contains("last_version = " + nextVersionExpression));
  }
}
