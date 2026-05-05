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

import org.apache.gravitino.storage.relational.mapper.IdpUserMetaMapper;
import org.apache.gravitino.storage.relational.mapper.provider.base.IdpUserMetaBaseSQLProvider;
import org.apache.ibatis.annotations.Param;

public class IdpUserMetaPostgreSQLProvider extends IdpUserMetaBaseSQLProvider {

  @Override
  public String softDeleteIdpUser(Long userId, Long deletedAt, String auditInfo) {
    return "UPDATE "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " SET deleted_at = CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT),"
        + " audit_info = #{auditInfo},"
        + " current_version = current_version + 1,"
        + " last_version = last_version + 1"
        + " WHERE user_id = #{userId} AND deleted_at = 0";
  }

  @Override
  public String deleteIdpUserMetasByLegacyTimeline(Long legacyTimeline, @Param("limit") int limit) {
    return "DELETE FROM "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " WHERE user_id IN (SELECT user_id FROM "
        + IdpUserMetaMapper.IDP_USER_TABLE_NAME
        + " WHERE deleted_at > 0 AND deleted_at < #{legacyTimeline} LIMIT #{limit})";
  }
}
