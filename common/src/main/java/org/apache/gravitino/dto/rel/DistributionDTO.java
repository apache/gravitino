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

package org.apache.gravitino.dto.rel;

import static org.apache.gravitino.dto.rel.expressions.FunctionArg.EMPTY_ARGS;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import org.apache.gravitino.dto.rel.expressions.FunctionArg;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Strategy;

/** Data transfer object representing distribution information. */
@JsonSerialize(using = JsonUtils.DistributionSerializer.class)
@JsonDeserialize(using = JsonUtils.DistributionDeserializer.class)
public class DistributionDTO implements Distribution {

  /** A DistributionDTO instance that represents no distribution. */
  public static final DistributionDTO NONE =
      builder().withStrategy(Strategy.NONE).withNumber(0).withArgs(EMPTY_ARGS).build();

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  // Distribution strategy/method
  private final Strategy strategy;

  // Number of buckets/distribution
  private final int number;

  private final FunctionArg[] args;

  private DistributionDTO(Strategy strategy, int number, FunctionArg[] args) {
    this.args = args;
    this.number = number;
    this.strategy = strategy;
  }

  /**
   * Returns the arguments of the function.
   *
   * @return The arguments of the function.
   */
  public FunctionArg[] args() {
    return args;
  }

  /**
   * Returns the strategy of the distribution.
   *
   * @return The strategy of the distribution.
   */
  @Override
  public Strategy strategy() {
    return strategy;
  }

  /**
   * Returns the number of buckets.
   *
   * @return The number of buckets.
   */
  @Override
  public int number() {
    return number;
  }

  /**
   * Returns the name of the distribution.
   *
   * @return The name of the distribution.
   */
  @Override
  public Expression[] expressions() {
    return args;
  }

  /**
   * Validates the distribution.
   *
   * @param columns The columns to be validated.
   * @throws IllegalArgumentException If the distribution is invalid.
   */
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(args).forEach(expression -> expression.validate(columns));
  }

  /** Builder for {@link DistributionDTO}. */
  public static class Builder {
    private FunctionArg[] args;
    private int number = 0;
    private Strategy strategy;

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Sets the arguments of the function.
     *
     * @param args The arguments of the function.
     * @return The builder.
     */
    public Builder withArgs(FunctionArg... args) {
      this.args = args;
      return this;
    }

    /**
     * Sets the number of buckets.
     *
     * @param bucketNum The number of buckets.
     * @return The builder.
     */
    public Builder withNumber(int bucketNum) {
      this.number = bucketNum;
      return this;
    }

    /**
     * Sets the strategy of the distribution.
     *
     * @param strategy The strategy of the distribution.
     * @return The builder.
     */
    public Builder withStrategy(Strategy strategy) {
      this.strategy = strategy;
      return this;
    }

    /**
     * Builds a new instance of {@link DistributionDTO}.
     *
     * @return The new instance.
     */
    public DistributionDTO build() {
      strategy = strategy == null ? Strategy.HASH : strategy;

      Preconditions.checkState(args != null, "expressions cannot be null");
      // Check if the number of buckets is greater than -1, -1 is auto.
      Preconditions.checkState(number >= -1, "bucketNum must be greater than or equal -1");
      return new DistributionDTO(strategy, number, args);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DistributionDTO)) {
      return false;
    }

    DistributionDTO that = (DistributionDTO) o;

    if (number != that.number) {
      return false;
    }
    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    if (!Arrays.equals(args, that.args)) {
      return false;
    }
    return strategy == that.strategy;
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(args);
    result = 31 * result + number;
    result = 31 * result + (strategy != null ? strategy.hashCode() : 0);
    return result;
  }
}
