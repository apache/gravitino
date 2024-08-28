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

import static org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper.TAG_METADATA_OBJECT_RELATION_TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.TagMetadataObjectRelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class TagMetadataObjectRelSQLProvider {

  private static final Map<JDBCBackendType, TagMetadataObjectRelBaseProvider>
      METALAKE_META_SQL_PROVIDER_MAP =
          ImmutableMap.of(
              JDBCBackendType.MYSQL, new TagMetadataObjectRelMySQLProvider(),
              JDBCBackendType.H2, new TagMetadataObjectRelH2Provider(),
              JDBCBackendType.PG, new TagMetadataObjectRelPGProvider());

  public static TagMetadataObjectRelBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class TagMetadataObjectRelMySQLProvider extends TagMetadataObjectRelBaseProvider {}

  static class TagMetadataObjectRelH2Provider extends TagMetadataObjectRelBaseProvider {}

  static class TagMetadataObjectRelPGProvider extends TagMetadataObjectRelBaseProvider {

    @Override
    public String softDeleteTagMetadataObjectRelsByMetalakeAndTagName(
        String metalakeName, String tagName) {
      return "UPDATE "
          + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
          + " te SET te.deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE te.tag_id IN (SELECT tm.tag_id FROM "
          + TagMetaMapper.TAG_TABLE_NAME
          + " tm WHERE tm.metalake_id IN (SELECT mm.metalake_id FROM "
          + MetalakeMetaMapper.TABLE_NAME
          + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
          + " AND tm.deleted_at = 0) AND te.deleted_at = 0";
    }

    @Override
    public String softDeleteTagMetadataObjectRelsByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + TAG_METADATA_OBJECT_RELATION_TABLE_NAME
          + " te SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE EXISTS (SELECT * FROM "
          + TagMetaMapper.TAG_TABLE_NAME
          + " tm WHERE tm.metalake_id = #{metalakeId} AND tm.tag_id = te.tag_id"
          + " AND tm.deleted_at = 0) AND te.deleted_at = 0";
    }
  }

  public String listTagPOsByMetadataObjectIdAndType(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType) {
    return getProvider().listTagPOsByMetadataObjectIdAndType(metadataObjectId, metadataObjectType);
  }

  public String getTagPOsByMetadataObjectAndTagName(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("tagName") String tagName) {
    return getProvider()
        .getTagPOsByMetadataObjectAndTagName(metadataObjectId, metadataObjectType, tagName);
  }

  public String listTagMetadataObjectRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().listTagMetadataObjectRelsByMetalakeAndTagName(metalakeName, tagName);
  }

  public String batchInsertTagMetadataObjectRels(
      @Param("tagRels") List<TagMetadataObjectRelPO> tagRelPOs) {
    return getProvider().batchInsertTagMetadataObjectRels(tagRelPOs);
  }

  public String batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject(
      @Param("metadataObjectId") Long metadataObjectId,
      @Param("metadataObjectType") String metadataObjectType,
      @Param("tagIds") List<Long> tagIds) {
    return getProvider()
        .batchDeleteTagMetadataObjectRelsByTagIdsAndMetadataObject(
            metadataObjectId, metadataObjectType, tagIds);
  }

  public String softDeleteTagMetadataObjectRelsByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().softDeleteTagMetadataObjectRelsByMetalakeAndTagName(metalakeName, tagName);
  }

  public String softDeleteTagMetadataObjectRelsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteTagMetadataObjectRelsByMetalakeId(metalakeId);
  }

  public String deleteTagEntityRelsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteTagEntityRelsByLegacyTimeline(legacyTimeline, limit);
  }
}
