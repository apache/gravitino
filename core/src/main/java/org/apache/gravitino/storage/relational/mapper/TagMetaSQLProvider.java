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

import static org.apache.gravitino.storage.relational.mapper.TagMetaMapper.TAG_TABLE_NAME;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.storage.relational.JDBCBackend.JDBCBackendType;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class TagMetaSQLProvider {

  private static final Map<JDBCBackendType, TagMetaBaseProvider> METALAKE_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new TagMetaMySQLProvider(),
          JDBCBackendType.H2, new TagMetaH2Provider(),
          JDBCBackendType.PG, new TagMetaPGProvider());

  public static TagMetaBaseProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return METALAKE_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class TagMetaMySQLProvider extends TagMetaBaseProvider {}

  static class TagMetaH2Provider extends TagMetaBaseProvider {}

  static class TagMetaPGProvider extends TagMetaBaseProvider {

    @Override
    public String softDeleteTagMetaByMetalakeAndTagName(String metalakeName, String tagName) {
      return "UPDATE "
          + TAG_TABLE_NAME
          + " tm SET tm.deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE tm.metalake_id IN ("
          + " SELECT mm.metalake_id FROM "
          + MetalakeMetaMapper.TABLE_NAME
          + " mm WHERE mm.metalake_name = #{metalakeName} AND mm.deleted_at = 0)"
          + " AND tm.tag_name = #{tagName} AND tm.deleted_at = 0";
    }

    @Override
    public String softDeleteTagMetasByMetalakeId(Long metalakeId) {
      return "UPDATE "
          + TAG_TABLE_NAME
          + " SET deleted_at = floor(extract(epoch from((current_timestamp - timestamp '1970-01-01 00:00:00')*1000))) "
          + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
    }
  }

  public String listTagPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return getProvider().listTagPOsByMetalake(metalakeName);
  }

  public String listTagPOsByMetalakeAndTagNames(
      @Param("metalakeName") String metalakeName, @Param("tagNames") List<String> tagNames) {
    return getProvider().listTagPOsByMetalakeAndTagNames(metalakeName, tagNames);
  }

  public String selectTagIdByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().selectTagIdByMetalakeAndName(metalakeName, tagName);
  }

  public String selectTagMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().selectTagMetaByMetalakeAndName(metalakeName, tagName);
  }

  public String insertTagMeta(@Param("tagMeta") TagPO tagPO) {
    return getProvider().insertTagMeta(tagPO);
  }

  public String insertTagMetaOnDuplicateKeyUpdate(@Param("tagMeta") TagPO tagPO) {
    return getProvider().insertTagMetaOnDuplicateKeyUpdate(tagPO);
  }

  public String updateTagMeta(
      @Param("newTagMeta") TagPO newTagPO, @Param("oldTagMeta") TagPO oldTagPO) {
    return getProvider().updateTagMeta(newTagPO, oldTagPO);
  }

  public String softDeleteTagMetaByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().softDeleteTagMetaByMetalakeAndTagName(metalakeName, tagName);
  }

  public String softDeleteTagMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteTagMetasByMetalakeId(metalakeId);
  }

  public String deleteTagMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteTagMetasByLegacyTimeline(legacyTimeline, limit);
  }
}
