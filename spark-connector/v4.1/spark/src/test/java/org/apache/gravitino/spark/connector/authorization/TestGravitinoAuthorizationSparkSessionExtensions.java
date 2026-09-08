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

package org.apache.gravitino.spark.connector.authorization;

import java.util.Collections;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.parser.ParameterContext;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests the parser this module injects. Spark 4.1 added {@code parsePlanWithParameters} to {@code
 * ParserInterface} and gave it a default body, so a wrapper that forgets to forward it compiles and
 * then silently drops the parameter context that {@code SparkSession.sql} passes.
 */
public class TestGravitinoAuthorizationSparkSessionExtensions {

  @AfterEach
  void clearDeniedTables() {
    AuthorizationTable.clear();
  }

  @Test
  void testParsePlanWithParametersForwardsTheParameterContext() throws ParseException {
    ParserInterface delegate = Mockito.mock(ParserInterface.class);
    ParameterContext parameterContext = Mockito.mock(ParameterContext.class);

    injectedParser(delegate).parsePlanWithParameters("SELECT ?", parameterContext);

    Mockito.verify(delegate).parsePlanWithParameters("SELECT ?", parameterContext);
    // The inherited default would come through parsePlan and lose the context, which leaves the
    // parameter markers unbound outside legacy parameter substitution.
    Mockito.verify(delegate, Mockito.never()).parsePlan(Mockito.anyString());
  }

  @Test
  void testParsePlanWithParametersClearsTheDeniedTablesFirst() throws ParseException {
    ParserInterface parser = injectedParser(Mockito.mock(ParserInterface.class));
    AuthorizationTable.deny(
        "t",
        "metalake.catalog.schema.t",
        Collections.singleton(Privilege.Name.SELECT_TABLE),
        new ForbiddenException("denied"));

    parser.parsePlanWithParameters("SELECT ?", Mockito.mock(ParameterContext.class));

    Assertions.assertFalse(
        AuthorizationTable.drainFailure().isPresent(),
        "a denial recorded before the parse must not carry into the plan parsed after it");
  }

  private static ParserInterface injectedParser(ParserInterface delegate) {
    SparkSessionExtensions extensions = new SparkSessionExtensions();
    new GravitinoAuthorizationSparkSessionExtensions().apply(extensions);
    // The injected builder ignores the session, so a null one is enough to reach the wrapper.
    return extensions.buildParser(null, delegate);
  }
}
