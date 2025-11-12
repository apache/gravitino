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
import org.apache.gravitino.storage.relational.mapper.provider.base.TagMetaBaseSQLProvider;
import org.apache.gravitino.storage.relational.mapper.provider.postgresql.TagMetaPostgreSQLProvider;
import org.apache.gravitino.storage.relational.po.TagPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

public class TagMetaSQLProviderFactory {

  private static final Map<JDBCBackendType, TagMetaBaseSQLProvider> TAG_META_SQL_PROVIDER_MAP =
      ImmutableMap.of(
          JDBCBackendType.MYSQL, new TagMetaMySQLProvider(),
          JDBCBackendType.H2, new TagMetaH2Provider(),
          JDBCBackendType.POSTGRESQL, new TagMetaPostgreSQLProvider());

  public static TagMetaBaseSQLProvider getProvider() {
    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();

    JDBCBackendType jdbcBackendType = JDBCBackendType.fromString(databaseId);
    return TAG_META_SQL_PROVIDER_MAP.get(jdbcBackendType);
  }

  static class TagMetaMySQLProvider extends TagMetaBaseSQLProvider {}

  static class TagMetaH2Provider extends TagMetaBaseSQLProvider {}

  public static String listTagPOsByMetalake(@Param("metalakeName") String metalakeName) {
    return getProvider().listTagPOsByMetalake(metalakeName);
  }

  public static String listTagPOsByMetalakeAndTagNames(
      @Param("metalakeName") String metalakeName, @Param("tagNames") List<String> tagNames) {
    return getProvider().listTagPOsByMetalakeAndTagNames(metalakeName, tagNames);
  }

  public static String selectTagIdByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().selectTagIdByMetalakeAndName(metalakeName, tagName);
  }

  public static String selectTagMetaByMetalakeAndName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().selectTagMetaByMetalakeAndName(metalakeName, tagName);
  }

  public static String insertTagMeta(@Param("tagMeta") TagPO tagPO) {
    return getProvider().insertTagMeta(tagPO);
  }

  public static String insertTagMetaOnDuplicateKeyUpdate(@Param("tagMeta") TagPO tagPO) {
    return getProvider().insertTagMetaOnDuplicateKeyUpdate(tagPO);
  }

  public static String updateTagMeta(
      @Param("newTagMeta") TagPO newTagPO, @Param("oldTagMeta") TagPO oldTagPO) {
    return getProvider().updateTagMeta(newTagPO, oldTagPO);
  }

  public static String softDeleteTagMetaByMetalakeAndTagName(
      @Param("metalakeName") String metalakeName, @Param("tagName") String tagName) {
    return getProvider().softDeleteTagMetaByMetalakeAndTagName(metalakeName, tagName);
  }

  public static String softDeleteTagMetasByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return getProvider().softDeleteTagMetasByMetalakeId(metalakeId);
  }

  public static String deleteTagMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return getProvider().deleteTagMetasByLegacyTimeline(legacyTimeline, limit);
  }

  public static String selectTagMetaByMetalakeIdAndName(
      @Param("metalakeId") Long metalakeId, @Param("name") String name) {
    return getProvider().selectTagMetaByMetalakeIdAndName(metalakeId, name);
  }

  public static String selectTagByTagId(@Param("tagId") Long tagId) {
    return getProvider().selectTagByTagId(tagId);
  }
}
