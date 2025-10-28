/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.storage.relational.mapper;

import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.UpdateProvider;

public interface TableVersionMapper {
  String TABLE_NAME = "table_version_info";

  @InsertProvider(type = TableVersionSQLProviderFactory.class, method = "insertTableVersion")
  void insertTableVersion(@Param("tablePO") TablePO tablePO);

  @InsertProvider(
      type = TableVersionSQLProviderFactory.class,
      method = "insertTableVersionOnDuplicateKeyUpdate")
  void insertTableVersionOnDuplicateKeyUpdate(@Param("tablePO") TablePO tablePO);

  @UpdateProvider(
      type = TableVersionSQLProviderFactory.class,
      method = "softDeleteTableVersionByTableIdAndVersion")
  void softDeleteTableVersionByTableIdAndVersion(
      @Param("tableId") Long tableId, @Param("version") Long version);

  @DeleteProvider(
      type = TableVersionSQLProviderFactory.class,
      method = "deleteTableVersionByLegacyTimeline")
  Integer deleteTableVersionByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
