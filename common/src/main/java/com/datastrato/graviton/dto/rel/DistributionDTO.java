/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.dto.rel;

import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;

@EqualsAndHashCode
@JsonPropertyOrder({"expressions", "distNum", "distMethod"})
@Getter
public class DistributionDTO {

  public enum DistributionMethod {
    HASH,
    EVEN,
    RANGE;

    @JsonCreator
    public static DistributionMethod fromString(String value) {
      return DistributionMethod.valueOf(value.toUpperCase());
    }

    @JsonValue
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  }

  @JsonProperty("expressions")
  @NonNull
  private final List<Expression> expressions;

  @JsonProperty("distNum")
  private final int distNum;

  @JsonProperty("distMethod")
  private final DistributionMethod distributionMethod;

  private DistributionDTO(
      @JsonProperty("expressions") List<Expression> expressions,
      @JsonProperty("distNum") int distNum,
      @JsonProperty("distMethod") DistributionMethod distributionMethod) {
    this.expressions = expressions;
    this.distNum = distNum;
    this.distributionMethod = distributionMethod;
  }

  public static class Builder {
    private List<Expression> expressions;
    private int bucketNum;
    private DistributionMethod distributionMethod;

    public Builder() {}

    public Builder withExpressions(List<Expression> expressions) {
      this.expressions = expressions;
      return this;
    }

    public Builder withDistNum(int bucketNum) {
      this.bucketNum = bucketNum;
      return this;
    }

    public Builder withDistMethod(DistributionMethod distributionMethod) {
      this.distributionMethod = distributionMethod;
      return this;
    }

    public DistributionDTO build() {
      // Defualt bucket method is HASH
      distributionMethod =
          distributionMethod == null ? DistributionMethod.HASH : distributionMethod;

      Preconditions.checkState(
          CollectionUtils.isNotEmpty(expressions), "expressions cannot be null or empty");
      Preconditions.checkState(bucketNum >= 0, "bucketNum must be greater than 0");
      return new DistributionDTO(expressions, bucketNum, distributionMethod);
    }
  }
}
