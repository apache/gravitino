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

package org.apache.gravitino.rel;

import static org.apache.gravitino.rel.expressions.NamedReference.field;

import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestDistribution {

  @Test
  void testDistribution() {
    Expression[] distributionArg1 = new Expression[] {field("field1")};
    Distribution bucket = Distributions.hash(1, distributionArg1);

    Assertions.assertEquals(Strategy.HASH, bucket.strategy());
    Assertions.assertEquals(1, bucket.number());
    Assertions.assertArrayEquals(distributionArg1, bucket.expressions());

    Expression[] distributionArg2 =
        new Expression[] {field("field1"), FunctionExpression.of("now")};
    bucket = Distributions.even(11111, distributionArg2);

    Assertions.assertEquals(Strategy.EVEN, bucket.strategy());
    Assertions.assertEquals(11111, bucket.number());
    Assertions.assertArrayEquals(distributionArg2, bucket.expressions());
  }
}
