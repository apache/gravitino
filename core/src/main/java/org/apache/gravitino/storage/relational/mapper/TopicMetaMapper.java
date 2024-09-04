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

import java.util.List;
import org.apache.gravitino.storage.relational.po.TopicPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface TopicMetaMapper {
  String TABLE_NAME = "topic_meta";

  @InsertProvider(type = TopicMetaProviderFactory.class, method = "insertTopicMeta")
  void insertTopicMeta(@Param("topicMeta") TopicPO topicPO);

  @InsertProvider(
      type = TopicMetaProviderFactory.class,
      method = "insertTopicMetaOnDuplicateKeyUpdate")
  void insertTopicMetaOnDuplicateKeyUpdate(@Param("topicMeta") TopicPO topicPO);

  @SelectProvider(type = TopicMetaProviderFactory.class, method = "listTopicPOsBySchemaId")
  List<TopicPO> listTopicPOsBySchemaId(@Param("schemaId") Long schemaId);

  @SelectProvider(
      type = TopicMetaProviderFactory.class,
      method = "selectTopicMetaBySchemaIdAndName")
  TopicPO selectTopicMetaBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("topicName") String topicName);

  @SelectProvider(type = TopicMetaProviderFactory.class, method = "selectTopicMetaById")
  TopicPO selectTopicMetaById(@Param("topicId") Long topicId);

  @UpdateProvider(type = TopicMetaProviderFactory.class, method = "updateTopicMeta")
  Integer updateTopicMeta(
      @Param("newTopicMeta") TopicPO newTopicPO, @Param("oldTopicMeta") TopicPO oldTopicPO);

  @SelectProvider(type = TopicMetaProviderFactory.class, method = "selectTopicIdBySchemaIdAndName")
  Long selectTopicIdBySchemaIdAndName(
      @Param("schemaId") Long schemaId, @Param("topicName") String name);

  @UpdateProvider(type = TopicMetaProviderFactory.class, method = "softDeleteTopicMetasByTopicId")
  Integer softDeleteTopicMetasByTopicId(@Param("topicId") Long topicId);

  @UpdateProvider(type = TopicMetaProviderFactory.class, method = "softDeleteTopicMetasByCatalogId")
  Integer softDeleteTopicMetasByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = TopicMetaProviderFactory.class,
      method = "softDeleteTopicMetasByMetalakeId")
  Integer softDeleteTopicMetasByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(type = TopicMetaProviderFactory.class, method = "softDeleteTopicMetasBySchemaId")
  Integer softDeleteTopicMetasBySchemaId(@Param("schemaId") Long schemaId);

  @DeleteProvider(
      type = TopicMetaProviderFactory.class,
      method = "deleteTopicMetasByLegacyTimeline")
  Integer deleteTopicMetasByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
