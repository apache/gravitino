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
package org.apache.gravitino.storage.relational.mapper.provider.h2;

import org.apache.gravitino.storage.relational.mapper.ModelVersionAliasRelMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.ModelVersionAliasRelBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class ModelVersionAliasRelH2SQLProvider extends ModelVersionAliasRelBaseSQLProvider {

  @Override
  public String softDeleteModelVersionAliasRelsByModelIdAndAlias(
      @Param("modelId") Long modelId, @Param("alias") String alias) {
    return "UPDATE "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " SET deleted_at = (UNIX_TIMESTAMP() * 1000.0)"
        + " + EXTRACT(MICROSECOND FROM CURRENT_TIMESTAMP(3)) / 1000"
        + " WHERE model_id = #{modelId} AND model_version = ("
        + " SELECT model_version FROM "
        + ModelVersionAliasRelMapper.TABLE_NAME
        + " WHERE model_id = #{modelId} AND model_version_alias = #{alias} AND deleted_at = 0)"
        + " AND deleted_at = 0";
  }
}
