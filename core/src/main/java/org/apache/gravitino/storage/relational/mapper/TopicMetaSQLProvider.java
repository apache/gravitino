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

import static org.apache.gravitino.storage.relational.mapper.TopicMetaMapper.TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class TopicMetaSQLProvider {

  private static final Map<JDBCBackendType, TopicMetaBaseProvider> METALAKE_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new TopicMetaMySQLProvider(),
          JDBCBackendType.H2, new TopicMetaH2Provider(),
          JDBCBackendType.PG, new TopicMetaPostgreSQLProvider());

  public static TopicMetaBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class TopicMetaMySQLProvider extends TopicMetaBaseProvider {}

  static class TopicMetaH2Provider extends TopicMetaBaseProvider {}

  static class TopicMetaPostgreSQLProvider extends TopicMetaBaseProvider {

    @Override
    public String softDeleteTopicMetasByTopicId(Long topicId) {
      return "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE topic_id = #{topicId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteTopicMetasByCatalogId(Long catalogId) {
      return "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE catalog_id = #{catalogId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteTopicMetasByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000)))"
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
    }

    @Override
    public String softDeleteTopicMetasBySchemaId(Long schemaId) {
      return "UPDATE "
          + TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE schema_id = #{schemaId} AND deleted_at = 0";
    }

    @Override
    public String insertTopicMetaOnDuplicateKeyUpdate(TopicPO topicPO) {
      return "INSERT INTO "
          + TABLE_NAME
          + "(topic_id, topic_name, metalake_id, catalog_id, schema_id,"
          + " comment, properties, audit_info, current_version, last_version,"
          + " deleted_at)"
          + " VALUES("
          + " #{topicMeta.topicId},"
          + " #{topicMeta.topicName},"
          + " #{topicMeta.metalakeId},"
          + " #{topicMeta.catalogId},"
          + " #{topicMeta.schemaId},"
          + " #{topicMeta.comment},"
          + " #{topicMeta.properties},"
          + " #{topicMeta.auditInfo},"
          + " #{topicMeta.currentVersion},"
          + " #{topicMeta.lastVersion},"
          + " #{topicMeta.deletedAt}"
          + " )"
          + " ON CONFLICT (topic_id) DO UPDATE SET"
          + " topic_name = #{topicMeta.topicName},"
          + " metalake_id = #{topicMeta.metalakeId},"
          + " catalog_id = #{topicMeta.catalogId},"
          + " schema_id = #{topicMeta.schemaId},"
          + " comment = #{topicMeta.comment},"
          + " properties = #{topicMeta.properties},"
          + " audit_info = #{topicMeta.auditInfo},"
          + " current_version = #{topicMeta.currentVersion},"
          + " last_version = #{topicMeta.lastVersion},"
          + " deleted_at = #{topicMeta.deletedAt}";
    }
  }

  public String insertTopicMeta(@Param("topicMeta") TopicPO topicPO) {
    return getProvider().insertTopicMeta(topicPO);
  }

  public String insertTopicMetaOnDuplicateKeyUpdate(@Param("topicMeta") TopicPO topicPO) {
    return getProvider().insertTopicMetaOnDuplicateKeyUpdate(topicPO);
  }

  public String listTopicPOsBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().listTopicPOsBySchemaId(schemaId);
  }

  public String selectTopicMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("topicName") String topicName) {
    return getProvider().selectTopicMetaBySchemaIdAndName(schemaId, topicName);
  }

  public String selectTopicMetaById(@Param("topicId") Long topicId) {
    return getProvider().selectTopicMetaById(topicId);
  }

  public String updateTopicMeta(
      @Param("newTopicMeta") TopicPO newTopicPO, @Param("oldTopicMeta") TopicPO oldTopicPO) {
    return getProvider().updateTopicMeta(newTopicPO, oldTopicPO);
  }

  public String selectTopicIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("topicName") String name) {
    return getProvider().selectTopicIdBySchemaIdAndName(schemaId, name);
  }

  public String softDeleteTopicMetasByTopicId(@Param("topicId") Long topicId) {
    return getProvider().softDeleteTopicMetasByTopicId(topicId);
  }

  public String softDeleteTopicMetasByCatalogId(@Param("catalogId") Long catalogId) {
    return getProvider().softDeleteTopicMetasByCatalogId(catalogId);
  }

  public String softDeleteTopicMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteTopicMetasByMetalakeId(metalakeId);
  }

  public String softDeleteTopicMetasBySchemaId(@Param("schemaId") Long schemaId) {
    return getProvider().softDeleteTopicMetasBySchemaId(schemaId);
  }

  public String deleteTopicMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteTopicMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
