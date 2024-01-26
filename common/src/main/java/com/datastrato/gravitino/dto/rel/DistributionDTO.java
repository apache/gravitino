/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.rel;

import static com.datastrato.gravitino.dto.rel.expressions.FunctionArg.EMPTY_ARGS;

import com.datastrato.gravitino.dto.rel.expressions.FunctionArg;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Strategy;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.util.Arrays;

@JsonSerialize(using = JsonUtils.DistributionSerializer.class)
@JsonDeserialize(using = JsonUtils.DistributionDeserializer.class)
public class DistributionDTO implements Distribution {

  public static final DistributionDTO NONE =
      builder().withStrategy(Strategy.NONE).withNumber(0).withArgs(EMPTY_ARGS).build();

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

  public FunctionArg[] args() {
    return args;
  }

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
    return args;
  }

  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    Arrays.stream(args).forEach(expression -> expression.validate(columns));
  }

  public static class Builder {
    private FunctionArg[] args;
    private int number = 0;
    private Strategy strategy;

    public Builder() {}

    public Builder withArgs(FunctionArg... args) {
      this.args = args;
      return this;
    }

    public Builder withNumber(int bucketNum) {
      this.number = bucketNum;
      return this;
    }

    public Builder withStrategy(Strategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public DistributionDTO build() {
      strategy = strategy == null ? Strategy.HASH : strategy;

      Preconditions.checkState(args != null, "expressions cannot be null");
      Preconditions.checkState(number >= 0, "bucketNum must be greater than 0");
      return new DistributionDTO(strategy, number, args);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
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
