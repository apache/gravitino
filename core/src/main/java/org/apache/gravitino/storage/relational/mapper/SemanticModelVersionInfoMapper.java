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

import org.apache.gravitino.storage.relational.po.SemanticModelVersionInfoPO;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;

/** A MyBatis mapper for creating Semantic Model version snapshots. */
public interface SemanticModelVersionInfoMapper {

  /** The Semantic Model version snapshot table name. */
  String TABLE_NAME = "semantic_model_version_info";

  /** Inserts a Semantic Model version snapshot. */
  @InsertProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "insertSemanticModelVersionInfo")
  void insertSemanticModelVersionInfo(
      @Param("semanticModelVersionInfo") SemanticModelVersionInfoPO versionInfoPO);

  /** Inserts or overwrites a Semantic Model version snapshot. */
  @InsertProvider(
      type = SemanticModelVersionInfoSQLProviderFactory.class,
      method = "insertSemanticModelVersionInfoOnDuplicateKeyUpdate")
  void insertSemanticModelVersionInfoOnDuplicateKeyUpdate(
      @Param("semanticModelVersionInfo") SemanticModelVersionInfoPO versionInfoPO);
}
