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
package org.apache.gravitino.storage.relational.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

/** Tests orphaned metadata-object relation collection. */
public class TestOrphanedMetadataObjectRelationService extends TestJDBCBackend {
  private static final long ORPHAN_ID = 987654321L;

  @TestTemplate
  public void testSoftDeleteOrphanedRelationsAndKeepLiveRelations() throws Exception {
    String metalake = "metalake_for_orphan_relation_test";
    String catalog = "catalog_for_orphan_relation_test";
    String schema = "schema_for_orphan_relation_test";
    createAndInsertMakeLake(metalake);
    createAndInsertCatalog(metalake, catalog);
    createAndInsertSchema(metalake, catalog, schema);
    TableEntity liveTable =
        createAndInsertTableEntity(Namespace.of(metalake, catalog, schema), "live_table");

    insertRelations(liveTable.id(), ORPHAN_ID);

    Assertions.assertEquals(
        5,
        OrphanedMetadataObjectRelationService.getInstance()
            .softDeleteOrphanedRelations(MetadataObject.Type.TABLE, 10));
    Assertions.assertEquals(5, countActiveRelations(liveTable.id()));
    Assertions.assertEquals(0, countActiveRelations(ORPHAN_ID));
    Assertions.assertEquals(
        0,
        OrphanedMetadataObjectRelationService.getInstance()
            .softDeleteOrphanedRelations(MetadataObject.Type.TABLE, 10));
  }

  private void insertRelations(long liveId, long orphanId) throws SQLException {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      for (long objectId : new long[] {liveId, orphanId}) {
        statement.executeUpdate(
            "INSERT INTO owner_meta (metalake_id, metadata_object_id, metadata_object_type,"
                + " owner_id, owner_type, audit_info, current_version, last_version, deleted_at,"
                + " updated_at) VALUES (1, "
                + objectId
                + ", 'TABLE', 1, 'USER', '{}', 0, 0, 0, 0)");
        statement.executeUpdate(
            "INSERT INTO tag_relation_meta (tag_id, metadata_object_id, metadata_object_type,"
                + " audit_info, current_version, last_version, deleted_at) VALUES ("
                + objectId
                + ", "
                + objectId
                + ", 'TABLE', '{}', 0, 0, 0)");
        statement.executeUpdate(
            "INSERT INTO policy_relation_meta (policy_id, metadata_object_id,"
                + " metadata_object_type, audit_info, current_version, last_version, deleted_at)"
                + " VALUES ("
                + objectId
                + ", "
                + objectId
                + ", 'TABLE', '{}', 0, 0, 0)");
        statement.executeUpdate(
            "INSERT INTO statistic_meta (statistic_id, statistic_name, statistic_value,"
                + " metalake_id, metadata_object_id, metadata_object_type, audit_info,"
                + " current_version, last_version, deleted_at) VALUES ("
                + objectId
                + ", 'row_count_"
                + objectId
                + "', '{}', 1, "
                + objectId
                + ", 'TABLE', '{}', 0, 0, 0)");
        statement.executeUpdate(
            "INSERT INTO role_meta_securable_object (role_id, metadata_object_id, type,"
                + " privilege_names, privilege_conditions, current_version, last_version,"
                + " deleted_at) VALUES ("
                + objectId
                + ", "
                + objectId
                + ", 'TABLE', 'SELECT_TABLE', '', 0, 0, 0)");
      }
    }
  }

  private int countActiveRelations(long objectId) throws SQLException {
    String query =
        "SELECT SUM(relation_count) FROM ("
            + "SELECT COUNT(*) relation_count FROM owner_meta WHERE metadata_object_id = "
            + objectId
            + " AND deleted_at = 0 UNION ALL "
            + "SELECT COUNT(*) FROM tag_relation_meta WHERE metadata_object_id = "
            + objectId
            + " AND deleted_at = 0 UNION ALL "
            + "SELECT COUNT(*) FROM policy_relation_meta WHERE metadata_object_id = "
            + objectId
            + " AND deleted_at = 0 UNION ALL "
            + "SELECT COUNT(*) FROM statistic_meta WHERE metadata_object_id = "
            + objectId
            + " AND deleted_at = 0 UNION ALL "
            + "SELECT COUNT(*) FROM role_meta_securable_object WHERE metadata_object_id = "
            + objectId
            + " AND deleted_at = 0) relation_counts";
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {
      Assertions.assertTrue(resultSet.next());
      return resultSet.getInt(1);
    }
  }
}
