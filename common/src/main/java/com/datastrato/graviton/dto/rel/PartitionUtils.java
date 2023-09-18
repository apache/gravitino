/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.rel;

import static com.datastrato.graviton.dto.rel.Partition.Strategy.IDENTITY;
import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_DAY;
import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_HOUR;
import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_LIST;
import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_MONTH;
import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_RANGE;
import static com.datastrato.graviton.rel.transforms.Transforms.NAME_OF_YEAR;
import static com.datastrato.graviton.rel.transforms.Transforms.field;
import static com.datastrato.graviton.rel.transforms.Transforms.function;
import static com.datastrato.graviton.rel.transforms.Transforms.identity;
import static com.datastrato.graviton.rel.transforms.Transforms.list;
import static com.datastrato.graviton.rel.transforms.Transforms.literal;
import static com.datastrato.graviton.rel.transforms.Transforms.range;
import static io.substrait.expression.ExpressionCreator.bool;
import static io.substrait.expression.ExpressionCreator.date;
import static io.substrait.expression.ExpressionCreator.decimal;
import static io.substrait.expression.ExpressionCreator.fixedChar;
import static io.substrait.expression.ExpressionCreator.fp32;
import static io.substrait.expression.ExpressionCreator.fp64;
import static io.substrait.expression.ExpressionCreator.i16;
import static io.substrait.expression.ExpressionCreator.i32;
import static io.substrait.expression.ExpressionCreator.i64;
import static io.substrait.expression.ExpressionCreator.i8;
import static io.substrait.expression.ExpressionCreator.string;
import static io.substrait.expression.ExpressionCreator.timestamp;
import static io.substrait.expression.ExpressionCreator.varChar;
import static java.time.ZoneOffset.UTC;

import com.datastrato.graviton.rel.transforms.Transform;
import com.datastrato.graviton.rel.transforms.Transforms;
import com.google.common.base.Preconditions;
import io.substrait.expression.AbstractExpressionVisitor;
import io.substrait.expression.Expression;
import io.substrait.function.ParameterizedTypeVisitor;
import io.substrait.type.Type;
import io.substrait.util.DecimalUtil;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class PartitionUtils {
  public static Transform[] toTransforms(Partition[] partitions) {
    if (partitions == null) {
      return null;
    }
    return Arrays.stream(partitions).map(PartitionUtils::toTransform).toArray(Transform[]::new);
  }

  public static Partition[] toPartitions(Transform[] transforms) {
    if (transforms == null) {
      return null;
    }
    return Arrays.stream(transforms).map(PartitionUtils::toPartition).toArray(Partition[]::new);
  }

  private static Transform toTransform(Partition partition) {
    switch (partition.strategy()) {
      case IDENTITY:
        return identity(((SimplePartitionDTO) partition).getFieldName());
      case YEAR:
      case MONTH:
      case DAY:
      case HOUR:
        return function(
            partition.strategy().name().toLowerCase(),
            new Transform[] {field(((SimplePartitionDTO) partition).getFieldName())});
      case LIST:
        // TODO(minghuang): add Assignments after Transform support partition value
        return list(((ListPartitionDTO) partition).getFieldNames());
      case RANGE:
        // TODO(minghuang): add Ranges after Transform support partition value
        return range(((RangePartitionDTO) partition).getFieldName());
      case EXPRESSION:
        return toTransform(((ExpressionPartitionDTO) partition).getExpression());
    }
    throw new IllegalArgumentException(
        "Unsupported partition type " + partition.getClass().getCanonicalName());
  }

  private static Transform toTransform(ExpressionPartitionDTO.Expression expression) {
    switch (expression.expressionType()) {
      case FIELD:
        return field(((ExpressionPartitionDTO.FieldExpression) expression).getFieldName());
      case LITERAL:
        return literal(getLiteral((ExpressionPartitionDTO.LiteralExpression) expression));
      case FUNCTION:
        return function(
            ((ExpressionPartitionDTO.FunctionExpression) expression).getFuncName(),
            Arrays.stream(((ExpressionPartitionDTO.FunctionExpression) expression).getArgs())
                .map(PartitionUtils::toTransform)
                .toArray(Transform[]::new));
    }
    throw new IllegalArgumentException(
        "Unsupported expression type " + expression.getClass().getCanonicalName());
  }

  private static Expression.Literal getLiteral(
      ExpressionPartitionDTO.LiteralExpression literalExpression) {
    LiteralConverter literalConverter = new LiteralConverter(literalExpression.getValue());
    return literalExpression.getType().accept(literalConverter);
  }

  private static class LiteralConverter
      extends ParameterizedTypeVisitor.ParameterizedTypeThrowsVisitor<
          Expression.Literal, RuntimeException> {
    private final String value;

    public LiteralConverter(String value) {
      super("Only support type literals and parameterized types.");
      this.value = value;
    }

    @Override
    public Expression.Literal visit(Type.Bool type) throws RuntimeException {
      return bool(type.nullable(), Boolean.parseBoolean(value));
    }

    @Override
    public Expression.Literal visit(Type.I8 type) throws RuntimeException {
      return i8(type.nullable(), Integer.parseInt(value));
    }

    @Override
    public Expression.Literal visit(Type.I16 type) throws RuntimeException {
      return i16(type.nullable(), Integer.parseInt(value));
    }

    @Override
    public Expression.Literal visit(Type.I32 type) throws RuntimeException {
      return i32(type.nullable(), Integer.parseInt(value));
    }

    @Override
    public Expression.Literal visit(Type.I64 type) throws RuntimeException {
      return i64(type.nullable(), Long.parseLong(value));
    }

    @Override
    public Expression.Literal visit(Type.FP32 type) throws RuntimeException {
      return fp32(type.nullable(), Float.parseFloat(value));
    }

    @Override
    public Expression.Literal visit(Type.FP64 type) throws RuntimeException {
      return fp64(type.nullable(), Double.parseDouble(value));
    }

    @Override
    public Expression.Literal visit(Type.Str type) throws RuntimeException {
      return string(type.nullable(), value);
    }

    @Override
    public Expression.Literal visit(Type.VarChar type) throws RuntimeException {
      return varChar(type.nullable(), value, type.length());
    }

    @Override
    public Expression.Literal visit(Type.FixedChar type) throws RuntimeException {
      return fixedChar(type.nullable(), value);
    }

    @Override
    public Expression.Literal visit(Type.Date type) throws RuntimeException {
      return date(type.nullable(), (int) LocalDate.parse(value).toEpochDay());
    }

    @Override
    public Expression.Literal visit(Type.Timestamp type) throws RuntimeException {
      return timestamp(type.nullable(), LocalDateTime.parse(value));
    }

    @Override
    public Expression.Literal visit(Type.Decimal type) throws RuntimeException {

      return decimal(type.nullable(), new BigDecimal(value), type.precision(), type.scale());
    }
  }

  private static Partition toPartition(Transform transform) {
    if (transform instanceof Transforms.NamedReference) {
      return new SimplePartitionDTO.Builder()
          .withStrategy(IDENTITY)
          .withFieldName(((Transforms.NamedReference) transform).value())
          .build();
    }

    if (transform instanceof Transforms.FunctionTrans) {
      Transform[] arguments = transform.arguments();
      switch (transform.name().toLowerCase()) {
        case NAME_OF_YEAR:
        case NAME_OF_MONTH:
        case NAME_OF_DAY:
        case NAME_OF_HOUR:
          if (arguments.length == 1 && arguments[0] instanceof Transforms.NamedReference) {
            return new SimplePartitionDTO.Builder()
                .withStrategy(Partition.Strategy.valueOf(transform.name().toUpperCase()))
                .withFieldName(((Transforms.NamedReference) arguments[0]).value())
                .build();
          }
        case NAME_OF_LIST:
          if (Arrays.stream(arguments).allMatch(arg -> arg instanceof Transforms.NamedReference)) {
            return new ListPartitionDTO.Builder()
                .withFieldNames(
                    Arrays.stream(arguments)
                        .map(arg -> ((Transforms.NamedReference) arg).value())
                        .toArray(String[][]::new))
                // TODO(minghuang): add Assignments after Transform support partition value
                .build();
          }
        case NAME_OF_RANGE:
          if (arguments.length == 1 && arguments[0] instanceof Transforms.NamedReference) {
            return new RangePartitionDTO.Builder()
                .withFieldName(((Transforms.NamedReference) arguments[0]).value())
                // TODO(minghuang): add Ranges after Transform support partition value
                .build();
          }
        default:
          return new ExpressionPartitionDTO.Builder(toExpression(transform)).build();
      }
    }

    throw new IllegalArgumentException(
        "Unsupported transform type " + transform.getClass().getCanonicalName());
  }

  private static ExpressionPartitionDTO.Expression toExpression(Transform transform) {
    if (transform instanceof Transforms.NamedReference) {
      return new ExpressionPartitionDTO.FieldExpression.Builder()
          .withFieldName(((Transforms.NamedReference) transform).value())
          .build();
    } else if (transform instanceof Transforms.LiteralReference) {
      Expression.Literal literal = ((Transforms.LiteralReference) transform).value();
      return new ExpressionPartitionDTO.LiteralExpression.Builder()
          .withType(literal.getType())
          .withValue(literal.accept(LiteralStringConverter.INSTANCE))
          .build();
    } else {
      return new ExpressionPartitionDTO.FunctionExpression.Builder()
          .withFuncName(transform.name())
          .withArgs(
              Arrays.stream(transform.arguments())
                  .map(PartitionUtils::toExpression)
                  .toArray(ExpressionPartitionDTO.Expression[]::new))
          .build();
    }
  }

  private static class LiteralStringConverter
      extends AbstractExpressionVisitor<String, UnsupportedOperationException> {

    public static final LiteralStringConverter INSTANCE = new LiteralStringConverter();

    @Override
    public String visitFallback(Expression expr) {
      throw new UnsupportedOperationException("Unsupported literal type: " + expr.getType());
    }

    @Override
    public String visit(Expression.NullLiteral expr) throws UnsupportedOperationException {
      return "null";
    }

    @Override
    public String visit(Expression.BoolLiteral expr) throws UnsupportedOperationException {
      return String.valueOf(expr.value());
    }

    @Override
    public String visit(Expression.I8Literal expr) throws UnsupportedOperationException {
      return String.valueOf(expr.value());
    }

    @Override
    public String visit(Expression.I16Literal expr) throws UnsupportedOperationException {
      return String.valueOf(expr.value());
    }

    @Override
    public String visit(Expression.I32Literal expr) throws UnsupportedOperationException {
      return String.valueOf(expr.value());
    }

    @Override
    public String visit(Expression.I64Literal expr) throws UnsupportedOperationException {
      return String.valueOf(expr.value());
    }

    @Override
    public String visit(Expression.FP32Literal expr) throws UnsupportedOperationException {
      return String.valueOf(expr.value());
    }

    @Override
    public String visit(Expression.FP64Literal expr) throws UnsupportedOperationException {
      return String.valueOf(expr.value());
    }

    @Override
    public String visit(Expression.StrLiteral expr) throws UnsupportedOperationException {
      return expr.value();
    }

    @Override
    public String visit(Expression.TimeLiteral expr) throws UnsupportedOperationException {
      return LocalTime.ofSecondOfDay(TimeUnit.MICROSECONDS.toSeconds(expr.value()))
          .format(DateTimeFormatter.ISO_LOCAL_TIME);
    }

    @Override
    public String visit(Expression.DateLiteral expr) throws UnsupportedOperationException {
      return LocalDate.ofEpochDay(expr.value()).format(DateTimeFormatter.ISO_LOCAL_DATE);
    }

    @Override
    public String visit(Expression.TimestampLiteral expr) throws UnsupportedOperationException {
      return LocalDateTime.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(expr.value()), 0, UTC)
          .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    @Override
    public String visit(Expression.TimestampTZLiteral expr) throws UnsupportedOperationException {
      return LocalDateTime.ofEpochSecond(TimeUnit.MICROSECONDS.toSeconds(expr.value()), 0, UTC)
          .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    @Override
    public String visit(Expression.UUIDLiteral expr) throws UnsupportedOperationException {
      return expr.value().toString();
    }

    @Override
    public String visit(Expression.FixedCharLiteral expr) throws UnsupportedOperationException {
      return expr.value();
    }

    @Override
    public String visit(Expression.VarCharLiteral expr) throws UnsupportedOperationException {
      return expr.value();
    }

    @Override
    public String visit(Expression.DecimalLiteral expr) throws UnsupportedOperationException {
      byte[] value = expr.value().toByteArray();
      BigDecimal decimal = DecimalUtil.getBigDecimalFromBytes(value, expr.scale(), 16);
      return decimal.toPlainString();
    }
  }

  public static void validateFieldExist(ColumnDTO[] columns, String[] fieldName)
      throws IllegalArgumentException {
    Preconditions.checkArgument(
        columns != null && columns.length != 0, "columns cannot be null or empty");

    List<ColumnDTO> partitionColumn =
        Arrays.stream(columns)
            .filter(c -> c.name().equals(fieldName[0]))
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        partitionColumn.size() == 1, "partition field %s not found in table", fieldName[0]);

    // TODO: should validate nested fieldName after column type support namedStruct
  }
}
