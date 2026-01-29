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
package org.apache.gravitino.storage.relational.mapper.provider.postgresql;

import static org.apache.gravitino.storage.relational.mapper.SecurableObjectMapper.SECURABLE_OBJECT_TABLE_NAME;

import java.util.Optional;
import org.apache.gravitino.storage.relational.mapper.provider.base.SecurableObjectBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class SecurableObjectPostgreSQLProvider extends SecurableObjectBaseSQLProvider {

  @Override
  public String deleteSecurableObjectsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + SECURABLE_OBJECT_TABLE_NAME
        + " WHERE id IN (SELECT id FROM "
        + SECURABLE_OBJECT_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }

  @Override
  protected String softDeleteSQL(Optional<String> tableAlias) {
    // PostgreSQL doesn't support alias prefix in SET clause
    return " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT) ";
  }
}
