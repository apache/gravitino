/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.rel;

import static com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.ExpressionType.FIELD;
import static com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.ExpressionType.FUNCTION;
import static com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.ExpressionType.LITERAL;

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
import lombok.Getter;
import org.apache.logging.log4j.util.Strings;

@EqualsAndHashCode(callSuper = false)
public class ExpressionPartitionDTO implements Partition {

  @Getter
  @JsonProperty("expression")
  private final Expression expression;

  @Override
  public Strategy strategy() {
    return Strategy.EXPRESSION;
  }

  @JsonCreator
  private ExpressionPartitionDTO(
      @JsonProperty("strategy") String strategy,
      @JsonProperty("expression") Expression expression) {
    Preconditions.checkArgument(expression != null, "expression cannot be null");
    this.expression = expression;
  }

  public static class Builder {
    private Expression expression;

    public Builder(Expression expression) {
      this.expression = expression;
    }

    public ExpressionPartitionDTO build() {
      return new ExpressionPartitionDTO(Strategy.EXPRESSION.name(), expression);
    }
  }

  enum ExpressionType {
    FIELD,
    LITERAL,
    FUNCTION,
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.DEDUCTION)
  @JsonSubTypes({
    @JsonSubTypes.Type(value = ExpressionPartitionDTO.FieldExpression.class),
    @JsonSubTypes.Type(value = ExpressionPartitionDTO.LiteralExpression.class),
    @JsonSubTypes.Type(value = ExpressionPartitionDTO.FunctionExpression.class),
  })
  public interface Expression {
    @JsonProperty("expressionType")
    ExpressionType expressionType();
  }

  @EqualsAndHashCode
  public static class FieldExpression implements Expression {

    @Getter
    @JsonProperty("fieldName")
    private final String[] fieldName;

    @JsonCreator
    private FieldExpression(
        @JsonProperty("expressionType") String expressionType,
        @JsonProperty("fieldName") String[] fieldName) {
      Preconditions.checkArgument(
          fieldName != null && fieldName.length != 0, "fieldName cannot be null or empty");
      this.fieldName = fieldName;
    }

    @Override
    public ExpressionType expressionType() {
      return FIELD;
    }

    public static class Builder {
      private String[] fieldName;

      public Builder withFieldName(String[] fieldName) {
        this.fieldName = fieldName;
        return this;
      }

      public FieldExpression build() {
        return new FieldExpression(FIELD.name(), fieldName);
      }
    }
  }

  @EqualsAndHashCode
  public static class LiteralExpression implements Expression {

    @Getter
    @JsonProperty("type")
    @JsonSerialize(using = JsonUtils.TypeSerializer.class)
    @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
    private final Type type;

    @Getter
    @JsonProperty("value")
    private final String value;

    @JsonCreator
    private LiteralExpression(
        @JsonProperty("expressionType") String expressionType,
        @JsonProperty("type") Type type,
        @JsonProperty("value") String value) {
      this.type = type;
      this.value = value;
    }

    @Override
    public ExpressionType expressionType() {
      return LITERAL;
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

      public LiteralExpression build() {
        return new LiteralExpression(LITERAL.name(), type, value);
      }
    }
  }

  @EqualsAndHashCode
  public static class FunctionExpression implements Expression {

    @Getter
    @JsonProperty("funcName")
    private final String funcName;

    @Getter
    @JsonProperty("args")
    private final Expression[] args;

    @JsonCreator
    private FunctionExpression(
        @JsonProperty("expressionType") String expressionType,
        @JsonProperty("funcName") String funcName,
        @JsonProperty("args") Expression[] args) {
      Preconditions.checkArgument(Strings.isNotBlank(funcName), "funcName cannot be null or empty");
      this.funcName = funcName;
      this.args = args;
    }

    @Override
    public ExpressionType expressionType() {
      return FUNCTION;
    }

    public static class Builder {
      private String funcName;
      private Expression[] args;

      public Builder withFuncName(String funcName) {
        this.funcName = funcName;
        return this;
      }

      public Builder withArgs(Expression[] args) {
        this.args = args;
        return this;
      }

      public FunctionExpression build() {
        return new FunctionExpression(FUNCTION.name(), funcName, args);
      }
    }
  }
}
