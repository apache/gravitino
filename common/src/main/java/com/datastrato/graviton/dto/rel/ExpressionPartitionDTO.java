/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.rel;

import static com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.ExprType.FILED;

import com.datastrato.graviton.json.JsonUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import io.substrait.type.Type;
import lombok.EqualsAndHashCode;
import org.apache.logging.log4j.util.Strings;

@EqualsAndHashCode(callSuper = false)
public class ExpressionPartitionDTO implements Partition {

  @JsonProperty("expr")
  private final Expr expr;

  @Override
  public Strategy strategy() {
    return Strategy.EXPRESSION;
  }

  @JsonCreator
  private ExpressionPartitionDTO(
      @JsonProperty("strategy") String strategy, @JsonProperty("expr") Expr expr) {
    Preconditions.checkArgument(expr != null, "expr cannot be null");
    this.expr = expr;
  }

  public static class Builder {
    private Expr expr;

    public Builder(Expr expr) {
      this.expr = expr;
    }

    public ExpressionPartitionDTO build() {
      return new ExpressionPartitionDTO(Strategy.EXPRESSION.name(), expr);
    }
  }

  enum ExprType {
    FILED,
    LITERAL,
    FUNCTION,
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
  @JsonSubTypes({
    @JsonSubTypes.Type(value = ExpressionPartitionDTO.FieldExpr.class),
    @JsonSubTypes.Type(value = ExpressionPartitionDTO.LiteralExpr.class),
    @JsonSubTypes.Type(value = ExpressionPartitionDTO.FunctionExpr.class),
  })
  public interface Expr {
    @JsonProperty("expr_type")
    ExprType exprType();
  }

  @EqualsAndHashCode
  public static class FieldExpr implements Expr {

    @JsonProperty("field_name")
    private final String[] fieldName;

    @JsonCreator
    private FieldExpr(
        @JsonProperty("expr_type") String exprType,
        @JsonProperty("field_name") String[] fieldName) {
      Preconditions.checkArgument(
          fieldName != null && fieldName.length != 0, "fieldName cannot be null or empty");
      this.fieldName = fieldName;
    }

    @Override
    public ExprType exprType() {
      return FILED;
    }

    public static class Builder {
      private String[] fieldName;

      public Builder withFieldName(String[] fieldName) {
        this.fieldName = fieldName;
        return this;
      }

      public FieldExpr build() {
        return new FieldExpr(FILED.name(), fieldName);
      }
    }
  }

  @EqualsAndHashCode
  public static class LiteralExpr implements Expr {

    @JsonProperty("type")
    private final Type type;

    @JsonProperty("value")
    private final String value;

    @JsonCreator
    private LiteralExpr(
        @JsonProperty("expr_type") String exprType,
        @JsonProperty("type")
            @JsonSerialize(using = JsonUtils.TypeSerializer.class)
            @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
            Type type,
        @JsonProperty("value") String value) {
      this.type = type;
      this.value = value;
    }

    @Override
    public ExprType exprType() {
      return ExprType.LITERAL;
    }

    public static class Builder {
      private Type type;
      private String value;

      public Builder withType(Type type) {
        this.type = type;
        return this;
      }

      public Builder withValue(String value) {
        this.value = value;
        return this;
      }

      public LiteralExpr build() {
        return new LiteralExpr(ExprType.LITERAL.name(), type, value);
      }
    }
  }

  @EqualsAndHashCode
  public static class FunctionExpr implements Expr {

    @JsonProperty("func_name")
    private final String funcName;

    @JsonProperty("args")
    private final Expr[] args;

    @JsonCreator
    private FunctionExpr(
        @JsonProperty("expr_type") String exprType,
        @JsonProperty("func_name") String funcName,
        @JsonProperty("args") Expr[] args) {
      Preconditions.checkArgument(!Strings.isBlank(funcName), "funcName cannot be null or empty");
      this.funcName = funcName;
      this.args = args;
    }

    @Override
    public ExprType exprType() {
      return ExprType.FUNCTION;
    }

    public static class Builder {
      private String funcName;
      private Expr[] args;

      public Builder withFuncName(String funcName) {
        this.funcName = funcName;
        return this;
      }

      public Builder withArgs(Expr[] args) {
        this.args = args;
        return this;
      }

      public FunctionExpr build() {
        return new FunctionExpr(ExprType.FUNCTION.name(), funcName, args);
      }
    }
  }
}
