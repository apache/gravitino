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
import org.apache.gravitino.storage.relational.po.FilesetMaxVersionPO;
import org.apache.gravitino.storage.relational.po.FilesetVersionPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

/**
 * A MyBatis Mapper for fileset version info operation SQLs.
 *
 * <p>This interface class is a specification defined by MyBatis. It requires this interface class
 * to identify the corresponding SQLs for execution. We can write SQLs in an additional XML file, or
 * write SQLs with annotations in this interface Mapper. See: <a
 * href="https://mybatis.org/mybatis-3/getting-started.html"></a>
 */
public interface FilesetVersionMapper {
  String VERSION_TABLE_NAME = "fileset_version_info";

  @InsertProvider(type = FilesetVersionSQLProviderFactory.class, method = "insertFilesetVersion")
  void insertFilesetVersion(@Param("filesetVersion") FilesetVersionPO filesetVersionPO);

  @InsertProvider(
      type = FilesetVersionSQLProviderFactory.class,
      method = "insertFilesetVersionOnDuplicateKeyUpdate")
  void insertFilesetVersionOnDuplicateKeyUpdate(
      @Param("filesetVersion") FilesetVersionPO filesetVersionPO);

  @UpdateProvider(
      type = FilesetVersionSQLProviderFactory.class,
      method = "softDeleteFilesetVersionsByMetalakeId")
  Integer softDeleteFilesetVersionsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = FilesetVersionSQLProviderFactory.class,
      method = "softDeleteFilesetVersionsByCatalogId")
  Integer softDeleteFilesetVersionsByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = FilesetVersionSQLProviderFactory.class,
      method = "softDeleteFilesetVersionsBySchemaId")
  Integer softDeleteFilesetVersionsBySchemaId(@Param("schemaId") Long schemaId);

  @UpdateProvider(
      type = FilesetVersionSQLProviderFactory.class,
      method = "softDeleteFilesetVersionsByFilesetId")
  Integer softDeleteFilesetVersionsByFilesetId(@Param("filesetId") Long filesetId);

  @DeleteProvider(
      type = FilesetVersionSQLProviderFactory.class,
      method = "deleteFilesetVersionsByLegacyTimeline")
  Integer deleteFilesetVersionsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);

  @SelectProvider(
      type = FilesetVersionSQLProviderFactory.class,
      method = "selectFilesetVersionsByRetentionCount")
  List<FilesetMaxVersionPO> selectFilesetVersionsByRetentionCount(
      @Param("versionRetentionCount") Long versionRetentionCount);

  @UpdateProvider(
      type = FilesetVersionSQLProviderFactory.class,
      method = "softDeleteFilesetVersionsByRetentionLine")
  Integer softDeleteFilesetVersionsByRetentionLine(
      @Param("filesetId") Long filesetId,
      @Param("versionRetentionLine") long versionRetentionLine,
      @Param("limit") int limit);
}
