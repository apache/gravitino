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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;

@JsonPropertyOrder({"expressions", "distributionNumber", "distributionMethod"})
@Getter
public class DistributionDTO {

  // NONE is used to indicate that there is no distribution.
  public static final DistributionDTO NONE =
      new DistributionDTO(Collections.EMPTY_LIST, 0, DistributionMethod.HASH);

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
  private final List<Expression> expressions;

  @JsonProperty("distributionNumber")
  private final int distributionNumber;

  @JsonProperty("distributionMethod")
  private final DistributionMethod distributionMethod;

  private DistributionDTO(
      @JsonProperty("expressions") List<Expression> expressions,
      @JsonProperty("distNum") int distributionNumber,
      @JsonProperty("distMethod") DistributionMethod distributionMethod) {
    this.expressions = expressions;
    this.distributionNumber = distributionNumber;
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
      // Default bucket method is HASH
      distributionMethod =
          distributionMethod == null ? DistributionMethod.HASH : distributionMethod;

      Preconditions.checkState(
          CollectionUtils.isNotEmpty(expressions), "expressions cannot be null or empty");
      Preconditions.checkState(bucketNum >= 0, "bucketNum must be greater than 0");
      return new DistributionDTO(expressions, bucketNum, distributionMethod);
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
    return distributionNumber == that.distributionNumber
        && Objects.equal(expressions, that.expressions)
        && distributionMethod == that.distributionMethod;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(expressions, distributionNumber, distributionMethod);
  }
}
