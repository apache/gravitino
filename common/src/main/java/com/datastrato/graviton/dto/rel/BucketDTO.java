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
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;

@EqualsAndHashCode
@JsonPropertyOrder({"expressions", "bucket_num", "bucket_method"})
public class BucketDTO {

  public enum BucketMethod {
    HASH,
    EVEN;

    @JsonCreator
    public static BucketMethod fromString(String value) {
      return BucketMethod.valueOf(value.toUpperCase());
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

  @JsonProperty("bucket_num")
  private final int bucketNum;

  @JsonProperty("bucket_method")
  private final BucketMethod bucketMethod;

  private BucketDTO(
      @JsonProperty("expressions") List<Expression> expressions,
      @JsonProperty("bucket_num") int bucketNum,
      @JsonProperty("bucket_method") BucketMethod bucketMethod) {
    this.expressions = expressions;
    this.bucketNum = bucketNum;
    this.bucketMethod = bucketMethod;
  }

  public static class Builder {
    private List<Expression> expressions;
    private int bucketNum;
    private BucketMethod bucketMethod;

    public Builder() {}

    public Builder withExpressions(List<Expression> expressions) {
      this.expressions = expressions;
      return this;
    }

    public Builder withBucketNum(int bucketNum) {
      this.bucketNum = bucketNum;
      return this;
    }

    public Builder withBucketMethod(BucketMethod bucketMethod) {
      this.bucketMethod = bucketMethod;
      return this;
    }

    public BucketDTO build() {
      // Defualt bucket method is HASH
      bucketMethod = bucketMethod == null ? BucketMethod.HASH : bucketMethod;

      Preconditions.checkState(
          CollectionUtils.isNotEmpty(expressions), "expressions cannot be null or empty");
      Preconditions.checkState(bucketNum >= 0, "bucketNum must be greater than 0");
      return new BucketDTO(expressions, bucketNum, bucketMethod);
    }
  }
}
