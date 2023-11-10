/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.rel;

import static com.datastrato.gravitino.rel.expressions.NamedReference.field;

import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.distributions.Strategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestDistribution {

  @Test
  void testDistribution() {
    Expression[] distributionArg1 = new Expression[] {field("field1")};
    Distribution bucket = Distributions.ofHash(1, distributionArg1);

    Assertions.assertEquals(Strategy.HASH, bucket.strategy());
    Assertions.assertEquals(1, bucket.number());
    Assertions.assertArrayEquals(distributionArg1, bucket.expressions());

    Expression[] distributionArg2 =
        new Expression[] {
          field("field1"), FunctionExpression.of("now", Expression.EMPTY_EXPRESSION)
        };
    bucket = Distributions.ofEVEN(11111, distributionArg2);

    Assertions.assertEquals(Strategy.EVEN, bucket.strategy());
    Assertions.assertEquals(11111, bucket.number());
    Assertions.assertArrayEquals(distributionArg2, bucket.expressions());
  }
}
