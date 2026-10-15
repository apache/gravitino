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
package org.apache.gravitino.rel.expressions.distributions;

import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDistributions {

  // A separate Distribution implementation, used to prove Distributions.isNone recognizes NONE
  // structurally across representations rather than by concrete type. DistributionDTO.NONE would be
  // the natural other representation, but DistributionDTO lives in the common module and the api
  // module does not depend on common (main or test), so it is not on this test classpath. An
  // anonymous implementation stands in for that cross-representation check.
  private static Distribution distributionOf(Strategy strategy, int number, Expression... exprs) {
    return new Distribution() {
      @Override
      public Strategy strategy() {
        return strategy;
      }

      @Override
      public int number() {
        return number;
      }

      @Override
      public Expression[] expressions() {
        return exprs;
      }
    };
  }

  @Test
  public void testIsNone() {
    Assertions.assertTrue(Distributions.isNone(Distributions.NONE));

    // A structurally equal NONE distribution from a different implementation also matches.
    Assertions.assertTrue(
        Distributions.isNone(distributionOf(Strategy.NONE, 0, Expression.EMPTY_EXPRESSION)));

    // A null distribution means "no distribution specified", which is treated as NONE.
    Assertions.assertTrue(Distributions.isNone(null));
    Assertions.assertFalse(Distributions.isNone(Distributions.HASH));
    Assertions.assertFalse(Distributions.isNone(Distributions.RANGE));
    Assertions.assertFalse(
        Distributions.isNone(Distributions.even(10, NamedReference.field("col"))));
    Assertions.assertFalse(
        Distributions.isNone(distributionOf(Strategy.NONE, 5, Expression.EMPTY_EXPRESSION)));
  }

  @Test
  public void testImplEqualityIsStructuralWithinSameClass() {
    Assertions.assertEquals(Distributions.NONE, Distributions.NONE);
    Assertions.assertNotEquals(
        Distributions.even(10, NamedReference.field("col")),
        Distributions.even(10, NamedReference.field("other")));
    Assertions.assertEquals(
        Distributions.even(10, NamedReference.field("col")),
        Distributions.even(10, NamedReference.field("col")));
  }
}
