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
package org.apache.gravitino.trino.connector;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Unit tests for the version-agnostic part of {@link GravitinoConstraint}. */
class TestGravitinoConstraint {

  private final ColumnHandle internalHandle = new ColumnHandle() {};
  private final GravitinoColumnHandle wrappedHandle =
      new GravitinoColumnHandle("c1", internalHandle);

  @Test
  void testSummaryKeysAreUnwrapped() {
    Domain domain = Domain.singleValue(BIGINT, 1L);
    Constraint constraint =
        new Constraint(TupleDomain.withColumnDomains(Map.of(wrappedHandle, domain)));

    GravitinoConstraint gravitinoConstraint = new GravitinoConstraint(constraint);

    assertThat(gravitinoConstraint.getSummary().getDomains().orElseThrow())
        .containsExactly(Map.entry(internalHandle, domain));
  }

  @Test
  void testAssignmentsAreUnwrappedAndExpressionIsDelegated() {
    ConnectorExpression expression = Constant.TRUE;
    Constraint constraint =
        new Constraint(TupleDomain.all(), expression, Map.of("c1", wrappedHandle));

    GravitinoConstraint gravitinoConstraint = new GravitinoConstraint(constraint);

    assertThat(gravitinoConstraint.getAssignments())
        .containsExactly(Map.entry("c1", internalHandle));
    assertThat(gravitinoConstraint.getExpression()).isSameAs(expression);
  }
}
