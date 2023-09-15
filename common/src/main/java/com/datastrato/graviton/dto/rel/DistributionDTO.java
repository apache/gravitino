/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.dto.rel;

import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.Expression;
import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.FieldExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;

@JsonPropertyOrder({"expressions", "number", "strategy"})
@Getter
public class DistributionDTO {

  // NONE is used to indicate that there is no distribution.
  public static final DistributionDTO NONE =
      new DistributionDTO(new Expression[0], 0, Strategy.HASH);

  /**
   * Create a distribution on a single column. Like distribute by (a) or (a, b), for complex like
   * distributing by (func(a), b) or (func(a), func(b)), please use {@link Builder} to create.
   *
   * @param columnName column name
   * @return
   */
  public static DistributionDTO nameReferenceDistribution(
      Strategy strategy, int number, String... columnName) {
    Expression[] expressions =
        Arrays.stream(columnName)
            .map(name -> new String[] {name})
            .map(f -> new FieldExpression.Builder().withFieldName(f).build())
            .toArray(Expression[]::new);
    return new DistributionDTO(expressions, number, strategy);
  }

  public enum Strategy {
    HASH,
    EVEN,
    RANGE;

    @JsonCreator
    public static Strategy fromString(String value) {
      return Strategy.valueOf(value.toUpperCase());
    }

    @JsonValue
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  }

  @JsonProperty("expressions")
  private final Expression[] expressions;

  // Number of buckets/distribution
  @JsonProperty("number")
  private final int number;

  // Distribution strategy/method
  @JsonProperty("strategy")
  private final Strategy strategy;

  private DistributionDTO(
      @JsonProperty("expressions") Expression[] expressions,
      @JsonProperty("number") int number,
      @JsonProperty("strategy") Strategy strategy) {
    this.expressions = expressions;
    this.number = number;
    this.strategy = strategy;
  }

  public static class Builder {
    private Expression[] expressions;
    private int number;
    private Strategy strategy;

    public Builder() {}

    public Builder withExpressions(Expression[] expressions) {
      this.expressions = expressions;
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

      Preconditions.checkState(
          ArrayUtils.isNotEmpty(expressions), "expressions cannot be null or empty");
      Preconditions.checkState(number >= 0, "bucketNum must be greater than 0");
      return new DistributionDTO(expressions, number, strategy);
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
    if (!Arrays.equals(expressions, that.expressions)) {
      return false;
    }
    return strategy == that.strategy;
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(expressions);
    result = 31 * result + number;
    result = 31 * result + (strategy != null ? strategy.hashCode() : 0);
    return result;
  }
}
