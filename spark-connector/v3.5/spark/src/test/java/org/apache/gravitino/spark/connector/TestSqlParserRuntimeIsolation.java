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
package org.apache.gravitino.spark.connector;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.gravitino.policy.sql.RestrictedSqlExpressionParserFacade;
import org.apache.spark.sql.catalyst.parser.SqlBaseLexer;
import org.apache.spark.sql.catalyst.parser.SqlBaseParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verifies Gravitino's shaded SQL parser and Spark's ANTLR parser can run in the same JVM. */
class TestSqlParserRuntimeIsolation {
  @Test
  void testSparkAndPolicyParsersCoexist() {
    // Initialize Spark's ANTLR runtime before loading Gravitino's generated parser.
    assertSparkSqlParses();
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedSqlExpressionParserFacade.validateRowFilter(
                "(region STRING) RETURNS BOOLEAN -> region = 'US'"));
    Assertions.assertDoesNotThrow(
        () ->
            RestrictedSqlExpressionParserFacade.validateColumnMask(
                "(value STRING) RETURNS STRING -> show_last_4(value)"));
    assertSparkSqlParses();
  }

  private static void assertSparkSqlParses() {
    SqlBaseLexer lexer = new SqlBaseLexer(CharStreams.fromString("SELECT 1"));
    SqlBaseParser parser = new SqlBaseParser(new CommonTokenStream(lexer));
    Assertions.assertNotNull(parser.singleStatement());
    Assertions.assertEquals(0, parser.getNumberOfSyntaxErrors());
  }
}
