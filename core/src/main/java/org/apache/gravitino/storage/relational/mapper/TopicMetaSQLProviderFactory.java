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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.TopicMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.TopicMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class TopicMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, TopicMetaBaseSQLProvider> TOPIC_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new TopicMetaMySQLProvider(),
          JDBCBackendType.H2, new TopicMetaH2Provider(),
          JDBCBackendType.POSTGRESQL, new TopicMetaPostgreSQLProvider());

  public static TopicMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return TOPIC_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class TopicMetaMySQLProvider extends TopicMetaBaseSQLProvider {}

  static class TopicMetaH2Provider extends TopicMetaBaseSQLProvider {}

  public static String insertTopicMeta(@Param("topicMeta") TopicPO topicPO) {
    return getProvider().insertTopicMeta(topicPO);
  }

  public static String insertTopicMetaOnDuplicateKeyUpdate(@Param("topicMeta") TopicPO topicPO) {
    return getProvider().insertTopicMetaOnDuplicateKeyUpdate(topicPO);
  }

  public static String listTopicPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().listTopicPOsBySchemaId(schemaId);
  }

  public static String selectTopicMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("topicName") String topicName) {
    return getProvider().selectTopicMetaBySchemaIdAndName(schemaId, topicName);
  }

  public static String selectTopicMetaById(@Param("topicId") Long topicId) {
    return getProvider().selectTopicMetaById(topicId);
  }

  public static String updateTopicMeta(
      @Param("newTopicMeta") TopicPO newTopicPO, @Param("oldTopicMeta") TopicPO oldTopicPO) {
    return getProvider().updateTopicMeta(newTopicPO, oldTopicPO);
  }

  public static String selectTopicIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("topicName") String name) {
    return getProvider().selectTopicIdBySchemaIdAndName(schemaId, name);
  }

  public static String softDeleteTopicMetasByTopicId(@Param("topicId") Long topicId) {
    return getProvider().softDeleteTopicMetasByTopicId(topicId);
  }

  public static String softDeleteTopicMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteTopicMetasByCatalogId(catalogId);
  }

  public static String softDeleteTopicMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteTopicMetasByMetalakeId(metalakeId);
  }

  public static String softDeleteTopicMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteTopicMetasBySchemaId(schemaId);
  }

  public static String deleteTopicMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteTopicMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
