/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.spark.connector.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.catalyst.analysis.RelationTimeTravel;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Test;
import scala.collection.JavaConverters;

@SuppressWarnings("deprecation") // JavaConverters is deprecated on Scala 2.13 but required on 2.12
public class TestViewRelationQualifier {

  private static final ViewRelationQualifier QUALIFIER =
      new ViewRelationQualifier("hive", Arrays.asList("db1"), name -> name.equals("other"));

  @Test
  void testQualifiesOneAndTwoPartNames() throws ParseException {
    assertEquals(
        Arrays.asList("hive.db1.t", "hive.db2.u"),
        relations("SELECT * FROM t JOIN db2.u ON t.id = u.id"));
  }

  @Test
  void testKeepsNamesThatStartWithACatalog() throws ParseException {
    assertEquals(Arrays.asList("other.db.t"), relations("SELECT * FROM other.db.t"));
  }

  @Test
  void testQualifiesNamesInSubqueries() throws ParseException {
    assertEquals(
        Arrays.asList("hive.db1.a", "hive.db1.b"),
        relations("SELECT * FROM a WHERE id IN (SELECT id FROM b)"));
  }

  @Test
  void testDoesNotQualifyCteReferences() throws ParseException {
    // The CTE named t shadows the table t; only the table inside the CTE body is qualified.
    assertEquals(
        Arrays.asList("hive.db1.base"),
        relations("WITH t AS (SELECT * FROM base) SELECT * FROM t"));
  }

  @Test
  void testQualifiesTimeTravelRelations() throws ParseException {
    assertEquals(Arrays.asList("hive.db1.t"), relations("SELECT * FROM t VERSION AS OF 1"));
  }

  @Test
  void testQualifiesNestedCtesUnderLegacyPolicy() throws ParseException {
    SQLConf conf = SQLConf.get();
    String key = SQLConf.LEGACY_CTE_PRECEDENCE_POLICY().key();
    String previous = conf.getConfString(key, null);
    conf.setConfString(key, "LEGACY");
    try {
      assertEquals(
          Arrays.asList("hive.db1.t"),
          relations("WITH a AS (WITH b AS (SELECT * FROM t) SELECT * FROM b) SELECT * FROM a"));
      assertEquals(
          Arrays.asList("hive.db1.a", "hive.db1.b"),
          relations("SELECT * FROM a WHERE id IN (WITH c AS (SELECT id FROM b) SELECT id FROM c)"));
    } finally {
      if (previous == null) {
        conf.unsetConf(key);
      } else {
        conf.setConfString(key, previous);
      }
    }
  }

  private static List<String> relations(String sql) throws ParseException {
    List<String> names = new ArrayList<>();
    collect(QUALIFIER.qualify(new CatalystSqlParser().parsePlan(sql)), names);
    return names;
  }

  private static void collect(LogicalPlan plan, List<String> names) {
    if (plan instanceof RelationTimeTravel) {
      collect(((RelationTimeTravel) plan).relation(), names);
    }
    if (plan instanceof UnresolvedRelation) {
      List<String> parts =
          JavaConverters.seqAsJavaList(((UnresolvedRelation) plan).multipartIdentifier());
      names.add(String.join(".", parts));
    }
    JavaConverters.seqAsJavaList(plan.children()).forEach(child -> collect(child, names));
    JavaConverters.seqAsJavaList(plan.expressions()).forEach(e -> collect(e, names));
  }

  private static void collect(Expression expression, List<String> names) {
    if (expression instanceof SubqueryExpression) {
      collect(((SubqueryExpression) expression).plan(), names);
    }
    JavaConverters.seqAsJavaList(expression.children()).forEach(child -> collect(child, names));
  }
}
