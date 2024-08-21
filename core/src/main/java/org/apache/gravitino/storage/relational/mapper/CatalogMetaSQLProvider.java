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

import static org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper.TABLE_NAME;

import org.apache.ibatis.annotations.Param;

public class CatalogMetaSQLProvider {

  public String listCatalogPOsByMetalakeId(@Param("metalakeId") Long metalakeId) {
    return "SELECT catalog_id as catalogId, catalog_name as catalogName,"
        + " metalake_id as metalakeId, type, provider,"
        + " catalog_comment as catalogComment, properties, audit_info as auditInfo,"
        + " current_version as currentVersion, last_version as lastVersion,"
        + " deleted_at as deletedAt"
        + " FROM "
        + TABLE_NAME
        + " WHERE metalake_id = #{metalakeId} AND deleted_at = 0";
  }
}
