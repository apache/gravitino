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
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.mapper.provider.base.TagMetadataObjectRelBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.TagMetadataObjectRelPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.TagMetadataObjectRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class TagMetadataObjectRelSQLProviderFactory {

  private static final Map<JDBCBackendType, TagMetadataObjectRelBaseSQLProvider>
      TAG_METADATA_OBJECT_RELATION_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new TagMetadataObjectRelMySQLProvider(),
              JDBCBackendType.H2, new TagMetadataObjectRelH2Provider(),
              JDBCBackendType.POSTGRESQL, new TagMetadataObjectRelPostgreSQLProvider());

  public static TagMetadataObjectRelBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return TAG_METADATA_OBJECT_RELATION_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class TagMetadataObjectRelMySQLProvider extends TagMetadataObjectRelBaseSQLProvider {}

  static class TagMetadataObjectRelH2Provider extends TagMetadataObjectRelBaseSQLProvider {}

  public static String listTagPOsByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return getProvider().listTagPOsByMetadataObjectIdAndType(metadataObjectId, metadataObjectType);
  }

  public static String getTagPOsByMetadataObjectAndTagName(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("tagName") String tagName) {
    return getProvider()
        .getTagPOsByMetadataObjectAndTagName(metadataObjectId, metadataObjectType, tagName);
  }

  public static String listTagMetadataObjectRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().listTagMetadataObjectRelsByMetalakeAndTagName(metalakeName, tagName);
  }

  public static String batchInsertTagMetadataObjectRels(
      @Param("tagRels") List<TagMetadataObjectRelPO> tagRelPOs) {
    return getProvider().batchInsertTagMetadataObjectRels(tagRelPOs);
  }

  public static String batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("tagIds") List<Long> tagIds) {
    return getProvider()
        .batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject(
            metadataObjectId, metadataObjectType, tagIds);
  }

  public static String softDeleteTagMetadataObjectRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().softDeleteTagMetadataObjectRelsByMetalakeAndTagName(metalakeName, tagName);
  }

  public static String softDeleteTagMetadataObjectRelsByMetalakeId(
      @Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteTagMetadataObjectRelsByMetalakeId(metalakeId);
  }

  public static String deleteTagEntityRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteTagEntityRelsByLegacyTimeline(legacyTimeline, limit);
  }
}
