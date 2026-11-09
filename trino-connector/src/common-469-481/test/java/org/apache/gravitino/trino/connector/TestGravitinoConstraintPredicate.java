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
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the {@code predicate()}/{@code getPredicateColumns()} overrides of {@link
 * GravitinoConstraint}, which exist on Trino versions before 482.
 */
class TestGravitinoConstraintPredicate {

  private final ColumnHandle internalHandle = new ColumnHandle() {};
  private final GravitinoColumnHandle wrappedHandle =
      new GravitinoColumnHandle("c1", internalHandle);

  @Test
  void testPredicateAndPredicateColumnsAreUnwrapped() {
    Predicate<Map<ColumnHandle, NullableValue>> predicate =
        values -> values.containsKey(internalHandle);
    Constraint constraint = new Constraint(TupleDomain.all(), predicate, Set.of(wrappedHandle));

    GravitinoConstraint gravitinoConstraint = new GravitinoConstraint(constraint);

    assertThat(gravitinoConstraint.getPredicateColumns()).contains(Set.of(internalHandle));
    Predicate<Map<ColumnHandle, NullableValue>> wrapped =
        gravitinoConstraint.predicate().orElseThrow();
    assertThat(wrapped).isInstanceOf(GravitinoPredicate.class);
    assertThat(wrapped.test(Map.of(internalHandle, NullableValue.asNull(BIGINT)))).isTrue();
    assertThat(wrapped.test(Map.of())).isFalse();
  }

  @Test
  void testMissingPredicateStaysEmpty() {
    GravitinoConstraint gravitinoConstraint =
        new GravitinoConstraint(new Constraint(TupleDomain.all()));

    assertThat(gravitinoConstraint.predicate()).isEmpty();
    assertThat(gravitinoConstraint.getPredicateColumns()).isEmpty();
  }
}
