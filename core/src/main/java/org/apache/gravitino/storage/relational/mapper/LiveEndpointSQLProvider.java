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

import org.apache.gravitino.Entity;
import org.apache.gravitino.storage.relational.JDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.annotations.Param;

/** SQL for locking a live endpoint row with the backend's shared-lock syntax. */
public class LiveEndpointSQLProvider {

  /** Builds a locking read for a whitelisted metadata-object table. */
  public static String lockLiveEndpoint(
      @Param("type") Entity.EntityType type, @Param("id") long id) {
    String table;
    String idColumn;
    switch (type) {
      case METALAKE:
        table = MetalakeMetaMapper.TABLE_NAME;
        idColumn = "metalake_id";
        break;
      case CATALOG:
        table = CatalogMetaMapper.TABLE_NAME;
        idColumn = "catalog_id";
        break;
      case SCHEMA:
        table = SchemaMetaMapper.TABLE_NAME;
        idColumn = "schema_id";
        break;
      case TABLE:
        table = TableMetaMapper.TABLE_NAME;
        idColumn = "table_id";
        break;
      case VIEW:
        table = ViewMetaMapper.TABLE_NAME;
        idColumn = "view_id";
        break;
      case SEMANTIC_MODEL:
        table = SemanticModelMetaMapper.TABLE_NAME;
        idColumn = "semantic_model_id";
        break;
      case FILESET:
        table = FilesetMetaMapper.META_TABLE_NAME;
        idColumn = "fileset_id";
        break;
      case TOPIC:
        table = TopicMetaMapper.TABLE_NAME;
        idColumn = "topic_id";
        break;
      case MODEL:
        table = ModelMetaMapper.TABLE_NAME;
        idColumn = "model_id";
        break;
      case FUNCTION:
        table = FunctionMetaMapper.TABLE_NAME;
        idColumn = "function_id";
        break;
      case USER:
        table = UserMetaMapper.USER_TABLE_NAME;
        idColumn = "user_id";
        break;
      case GROUP:
        table = GroupMetaMapper.GROUP_TABLE_NAME;
        idColumn = "group_id";
        break;
      case ROLE:
        table = RoleMetaMapper.ROLE_TABLE_NAME;
        idColumn = "role_id";
        break;
      case TAG:
        table = TagMetaMapper.TAG_TABLE_NAME;
        idColumn = "tag_id";
        break;
      case POLICY:
        table = PolicyMetaMapper.POLICY_META_TABLE_NAME;
        idColumn = "policy_id";
        break;
      case JOB_TEMPLATE:
        table = JobTemplateMetaMapper.TABLE_NAME;
        idColumn = "job_template_id";
        break;
      case JOB:
        table = JobMetaMapper.TABLE_NAME;
        idColumn = "job_run_id";
        break;
      default:
        throw new IllegalArgumentException("Unsupported live endpoint type: " + type);
    }

    String databaseId =
        SqlSessionFactoryHelper.getInstance()
            .getSqlSessionFactory()
            .getConfiguration()
            .getDatabaseId();
    JDBCBackend.JDBCBackendType backend = JDBCBackend.JDBCBackendType.fromString(databaseId);
    String lockClause;
    switch (backend) {
      case H2:
        lockClause = " FOR UPDATE";
        break;
      case MYSQL:
        lockClause = " LOCK IN SHARE MODE";
        break;
      case POSTGRESQL:
        lockClause = " FOR SHARE";
        break;
      default:
        throw new IllegalArgumentException("Unsupported JDBC backend: " + backend);
    }
    return "SELECT "
        + idColumn
        + " FROM "
        + table
        + " WHERE "
        + idColumn
        + " = #{id} AND deleted_at = 0"
        + lockClause;
  }
}
