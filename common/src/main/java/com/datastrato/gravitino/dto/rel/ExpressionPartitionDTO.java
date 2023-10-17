/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel;

import static com.datastrato.gravitino.dto.rel.ExpressionPartitionDTO.ExpressionType.FIELD;
import static com.datastrato.gravitino.dto.rel.ExpressionPartitionDTO.ExpressionType.FUNCTION;
import static com.datastrato.gravitino.dto.rel.ExpressionPartitionDTO.ExpressionType.LITERAL;
import static com.datastrato.gravitino.rel.transforms.Transforms.NAME_OF_BUCKET;
import static com.datastrato.gravitino.rel.transforms.Transforms.NAME_OF_TRUNCATE;

import com.datastrato.gravitino.json.JsonUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
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

  @Override
  public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
    validateExpression(columns, expression);
  }

  private void validateExpression(ColumnDTO[] columns, Expression expression) {
    if (expression == null) {
      return;
    }

    switch (expression.expressionType()) {
      case FIELD:
        FieldExpression fieldExpression = (FieldExpression) expression;
        PartitionUtils.validateFieldExistence(columns, fieldExpression.fieldName);
        break;
      case FUNCTION:
        validateExpression(columns, expression);
        break;
    }
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

  public static ExpressionPartitionDTO bucket(String[] fieldName, int numBuckets) {
    return new ExpressionPartitionDTO.Builder(
            new FunctionExpression.Builder()
                .withFuncName(NAME_OF_BUCKET)
                .withArgs(
                    new Expression[] {
                      new FieldExpression.Builder().withFieldName(fieldName).build(),
                      new LiteralExpression.Builder()
                          .withType(TypeCreator.REQUIRED.I32)
                          .withValue(String.valueOf(numBuckets))
                          .build()
                    })
                .build())
        .build();
  }

  public static ExpressionPartitionDTO truncate(String[] fieldName, int width) {
    return new ExpressionPartitionDTO.Builder(
            new FunctionExpression.Builder()
                .withFuncName(NAME_OF_TRUNCATE)
                .withArgs(
                    new Expression[] {
                      new FieldExpression.Builder().withFieldName(fieldName).build(),
                      new LiteralExpression.Builder()
                          .withType(TypeCreator.REQUIRED.I32)
                          .withValue(String.valueOf(width))
                          .build()
                    })
                .build())
        .build();
  }

  enum ExpressionType {
    @JsonProperty("field")
    FIELD,
    @JsonProperty("literal")
    LITERAL,
    @JsonProperty("function")
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
  @Getter
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
  @Getter
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
  @Getter
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
