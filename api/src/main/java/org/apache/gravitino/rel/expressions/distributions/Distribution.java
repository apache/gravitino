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

import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.rel.expressions.Expression;

/** An interface that defines how data is distributed across partitions. */
@Evolving
public interface Distribution extends Expression {

  /**
   * @return the distribution strategy name.
   */
  Strategy strategy();

  /**
   * @return The number of buckets/distribution. For example, if the distribution strategy is HASH
   *     and the number is 10, then the data is distributed across 10 buckets.
   */
  int number();

  /**
   * @return The expressions passed to the distribution function.
   */
  Expression[] expressions();

  @Override
  default Expression[] children() {
    return expressions();
  }

  // Note: this interface intentionally does NOT define a `boolean equals(Distribution)` overload.
  // Such an overload never overrides Object.equals, so it is invisible to HashSet/HashMap and any
  // code comparing through Object references, which makes structural equality silently
  // dispatch-dependent. Implementations must override equals(Object)/hashCode() themselves
  // (DistributionImpl and DistributionDTO do). Use Distributions.isNone to test for the NONE
  // distribution across representations.
}
