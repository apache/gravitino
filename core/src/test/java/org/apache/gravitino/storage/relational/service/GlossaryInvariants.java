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
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import org.apache.gravitino.meta.GlossaryTermNames;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;

/** Checks the T2 storage invariants directly, independently of entity decoding. */
public final class GlossaryInvariants {
  private GlossaryInvariants() {}
  /** Checks normalized unique names and live synonym endpoints. */
  public static void assertDefinitions() throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      Set<String> names = new HashSet<>();
      try (ResultSet rows =
          statement.executeQuery(
              "SELECT metalake_id, name, name_key FROM glossary_term_meta WHERE deleted_at=0")) {
        while (rows.next()) {
          Assertions.assertEquals(GlossaryTermNames.key(rows.getString(2)), rows.getString(3));
          Assertions.assertTrue(names.add(rows.getLong(1) + ":" + rows.getString(3)));
        }
      }
      try (ResultSet rows =
          statement.executeQuery(
              "SELECT s.synonym, s.synonym_key, t.term_id FROM glossary_term_synonym s LEFT JOIN glossary_term_meta t ON s.term_id=t.term_id AND s.metalake_id=t.metalake_id AND t.deleted_at=0 WHERE s.deleted_at=0")) {
        while (rows.next()) {
          Assertions.assertEquals(GlossaryTermNames.key(rows.getString(1)), rows.getString(2));
          Assertions.assertNotNull(
              rows.getObject(3), "Live synonym must reference a live term in its metalake");
        }
      }
    }
  }

  /** Checks canonical relation keys, live endpoints and the unique mutex rows. */
  public static void assertRelations() throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      Set<String> keys = new HashSet<>();
      try (ResultSet rows =
          statement.executeQuery(
              "SELECT r.from_term_id, r.to_term_id, r.rel_type, f.term_id, t.term_id, m.metalake_id FROM glossary_term_rel r "
                  + "LEFT JOIN glossary_term_meta f ON f.term_id=r.from_term_id AND f.metalake_id=r.metalake_id AND f.deleted_at=0 "
                  + "LEFT JOIN glossary_term_meta t ON t.term_id=r.to_term_id AND t.metalake_id=r.metalake_id AND t.deleted_at=0 "
                  + "LEFT JOIN metalake_meta m ON m.metalake_id=r.metalake_id AND m.deleted_at=0 WHERE r.deleted_at=0")) {
        while (rows.next()) {
          Assertions.assertNotEquals(rows.getLong(1), rows.getLong(2));
          Assertions.assertTrue(
              keys.add(rows.getLong(1) + ":" + rows.getLong(2) + ":" + rows.getString(3)));
          Assertions.assertTrue(Set.of("BROADER", "RELATED").contains(rows.getString(3)));
          if ("RELATED".equals(rows.getString(3)))
            Assertions.assertTrue(rows.getLong(1) < rows.getLong(2));
          for (int i = 4; i <= 6; i++)
            Assertions.assertNotNull(
                rows.getObject(i), "Live relation requires live endpoints and metalake");
        }
      }
      Set<Long> metalakes = new HashSet<>();
      try (ResultSet rows =
          statement.executeQuery("SELECT metalake_id FROM glossary_hierarchy_lock")) {
        while (rows.next()) Assertions.assertTrue(metalakes.add(rows.getLong(1)));
      }
    }
  }

  /** Checks that every live membership row joins a live term and a live domain of its metalake. */
  public static void assertDomainMembership() throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery(
                "SELECT r.member_id, r.domain_id, t.term_id, d.domain_id FROM domain_member_rel r "
                    + "LEFT JOIN glossary_term_meta t ON t.term_id=r.member_id AND t.metalake_id=r.metalake_id AND t.deleted_at=0 "
                    + "LEFT JOIN domain_meta d ON d.domain_id=r.domain_id AND d.metalake_id=r.metalake_id AND d.deleted_at=0 "
                    + "WHERE r.member_type='GLOSSARY_TERM' AND r.deleted_at=0")) {
      while (rows.next()) {
        Assertions.assertNotNull(
            rows.getObject(3), "Live membership of term " + rows.getLong(1) + " needs a live term");
        Assertions.assertNotNull(
            rows.getObject(4),
            "Live membership in domain " + rows.getLong(2) + " needs a live domain");
      }
    }
  }

  /**
   * Checks that every live binding row has non-null key fields, unique live keys, a live term in
   * the same metalake, and a live target of its declared target type.
   */
  public static void assertBindings() throws Exception {
    try (SqlSession session =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession();
        Connection connection = session.getConnection();
        Statement statement = connection.createStatement()) {
      Set<String> keys = new HashSet<>();
      try (ResultSet rows =
          statement.executeQuery(
              "SELECT b.metalake_id, b.term_id, b.target_type, b.target_id, t.term_id,"
                  + " CASE b.target_type"
                  + "   WHEN 'CATALOG' THEN (SELECT c.catalog_id FROM catalog_meta c WHERE c.catalog_id=b.target_id AND c.metalake_id=b.metalake_id AND c.deleted_at=0)"
                  + "   WHEN 'SCHEMA' THEN (SELECT s.schema_id FROM schema_meta s WHERE s.schema_id=b.target_id AND s.metalake_id=b.metalake_id AND s.deleted_at=0)"
                  + "   WHEN 'TABLE' THEN (SELECT tb.table_id FROM table_meta tb WHERE tb.table_id=b.target_id AND tb.metalake_id=b.metalake_id AND tb.deleted_at=0)"
                  + "   WHEN 'VIEW' THEN (SELECT v.view_id FROM view_meta v WHERE v.view_id=b.target_id AND v.metalake_id=b.metalake_id AND v.deleted_at=0)"
                  + "   WHEN 'COLUMN' THEN (SELECT c.column_id FROM table_column_version_info c"
                  + "     JOIN table_meta tb ON tb.table_id=c.table_id AND tb.deleted_at=0"
                  + "     WHERE c.column_id=b.target_id AND c.metalake_id=b.metalake_id AND c.deleted_at=0"
                  + "     AND c.table_version=(SELECT MAX(c2.table_version) FROM table_column_version_info c2 WHERE c2.column_id=c.column_id AND c2.deleted_at=0)"
                  + "     AND c.column_op_type<>3 LIMIT 1)"
                  + "   WHEN 'FILESET' THEN (SELECT f.fileset_id FROM fileset_meta f WHERE f.fileset_id=b.target_id AND f.metalake_id=b.metalake_id AND f.deleted_at=0)"
                  + "   WHEN 'TOPIC' THEN (SELECT tp.topic_id FROM topic_meta tp WHERE tp.topic_id=b.target_id AND tp.metalake_id=b.metalake_id AND tp.deleted_at=0)"
                  + "   WHEN 'MODEL' THEN (SELECT m.model_id FROM model_meta m WHERE m.model_id=b.target_id AND m.metalake_id=b.metalake_id AND m.deleted_at=0)"
                  + "   WHEN 'METRIC' THEN (SELECT sm.metric_id FROM semantic_model_metric_meta sm"
                  + "     JOIN semantic_model_meta model ON model.semantic_model_id=sm.semantic_model_id AND model.deleted_at=0"
                  + "     WHERE sm.metric_id=b.target_id AND sm.metalake_id=b.metalake_id AND sm.deleted_at=0)"
                  + "   ELSE NULL END AS live_target_id"
                  + " FROM glossary_term_binding b"
                  + " LEFT JOIN glossary_term_meta t ON t.term_id=b.term_id AND t.metalake_id=b.metalake_id AND t.deleted_at=0"
                  + " WHERE b.deleted_at=0")) {
        while (rows.next()) {
          Assertions.assertNotNull(rows.getObject(2), "term_id must not be null");
          Assertions.assertNotNull(rows.getObject(3), "target_type must not be null");
          Assertions.assertNotNull(rows.getObject(4), "target_id must not be null");
          String key = rows.getLong(2) + ":" + rows.getString(3) + ":" + rows.getLong(4);
          Assertions.assertTrue(keys.add(key), "Duplicate live binding: " + key);
          Assertions.assertNotNull(
              rows.getObject(5), "Live binding must reference a live term: " + key);
          Assertions.assertNotNull(
              rows.getObject(6), "Live binding must reference a live target: " + key);
        }
      }
    }
  }
}
