/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.po.OntologySnapshotPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/** Tests ontology snapshot persistence through the registered MyBatis mapper. */
class TestOntologyMapper extends TestJDBCBackend {
  private static final long ONTOLOGY_ID = 41L;
  private static final String NAME = "Sales";

  private long metalakeId;

  @BeforeEach
  void createOntology() throws IOException {
    metalakeId = createAndInsertMakeLake("lake").id();
    try (SqlSession session = openSession()) {
      OntologyMapper mapper = session.getMapper(OntologyMapper.class);
      OntologySnapshotPO first = snapshot(1, 1);
      mapper.insertOntology(first);
      mapper.appendOntologyVersion(first);
      session.commit();
    }
  }

  @TestTemplate
  void testSnapshotRoundTripAndStaleUpdate() {
    try (SqlSession session = openSession()) {
      OntologyMapper mapper = session.getMapper(OntologyMapper.class);
      assertSnapshot(mapper.loadOntology(metalakeId, NAME), 1);

      OntologySnapshotPO second = snapshot(1, 2);
      mapper.appendOntologyVersion(second);
      assertEquals(1, mapper.updateOntology(second));
      assertEquals(0, mapper.updateOntology(snapshot(1, 3)));
      session.commit();

      assertSnapshot(mapper.loadOntologyById(ONTOLOGY_ID), 2);
    }
  }

  @TestTemplate
  void testRetireThenPurgeOldSnapshots() throws SQLException {
    try (SqlSession session = openSession()) {
      OntologyMapper mapper = session.getMapper(OntologyMapper.class);
      for (int version = 2; version <= 3; version++) {
        OntologySnapshotPO next = snapshot(version - 1, version);
        mapper.appendOntologyVersion(next);
        assertEquals(1, mapper.updateOntology(next));
      }

      long retiredAt = 100L;
      assertEquals(1, mapper.retireOntologyVersions(2, 10, retiredAt));
      assertEquals(Map.of(1, retiredAt, 2, 0L, 3, 0L), versionDeletionTimes(session));
      assertEquals(0, mapper.purgeOntologyVersions(retiredAt, 10));
      assertEquals(1, mapper.purgeOntologyVersions(retiredAt + 1, 10));
      session.commit();

      assertEquals(Map.of(2, 0L, 3, 0L), versionDeletionTimes(session));
      assertSnapshot(mapper.loadOntology(metalakeId, NAME), 3);
    }
  }

  private static SqlSession openSession() {
    return SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
  }

  private OntologySnapshotPO snapshot(int currentVersion, int lastVersion) {
    OntologySnapshotPO row = new OntologySnapshotPO();
    row.setId(ONTOLOGY_ID);
    row.setName(NAME);
    row.setMetalakeId(metalakeId);
    row.setCurrentVersion(currentVersion);
    row.setLastVersion(lastVersion);
    row.setComment("Comment " + lastVersion);
    row.setDefinition(
        "{\"ontology\":[{\"concept\":\"Sales" + lastVersion + "\",\"type\":\"EntityType\"}]}");
    row.setProperties("{\"owner\":\"team" + lastVersion + "\"}");
    row.setAuditInfo("{\"creator\":\"user" + lastVersion + "\"}");
    return row;
  }

  private void assertSnapshot(OntologySnapshotPO actual, int version) {
    OntologySnapshotPO expected = snapshot(version, version);
    assertNotNull(actual);
    assertEquals(ONTOLOGY_ID, actual.getId());
    assertEquals(NAME, actual.getName());
    assertEquals(metalakeId, actual.getMetalakeId());
    assertEquals(version, actual.getCurrentVersion());
    assertEquals(version, actual.getLastVersion());
    assertEquals(expected.getComment(), actual.getComment());
    assertEquals(expected.getDefinition(), actual.getDefinition());
    assertEquals(expected.getProperties(), actual.getProperties());
    assertEquals(expected.getAuditInfo(), actual.getAuditInfo());
  }

  private static Map<Integer, Long> versionDeletionTimes(SqlSession session) throws SQLException {
    try (PreparedStatement statement =
        session
            .getConnection()
            .prepareStatement(
                "SELECT version, deleted_at FROM ontology_version_info WHERE ontology_id = ?")) {
      statement.setLong(1, ONTOLOGY_ID);
      try (ResultSet rows = statement.executeQuery()) {
        Map<Integer, Long> times = new HashMap<>();
        while (rows.next()) {
          times.put(rows.getInt("version"), rows.getLong("deleted_at"));
        }
        return times;
      }
    }
  }
}
