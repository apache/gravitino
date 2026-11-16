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
}
