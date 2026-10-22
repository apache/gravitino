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
package org.apache.gravitino.iceberg.service.restriction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.gravitino.policy.rego.CanonicalExpression;
import org.apache.gravitino.policy.rego.Column;
import org.apache.gravitino.policy.rego.Comparison;
import org.apache.gravitino.policy.rego.ComparisonOperator;
import org.apache.gravitino.policy.rego.GroupMembership;
import org.apache.gravitino.policy.rego.Literal;
import org.apache.gravitino.policy.rego.LiteralArray;
import org.apache.gravitino.policy.rego.LiteralType;
import org.apache.gravitino.policy.rego.LogicalExpression;
import org.apache.gravitino.policy.rego.LogicalOperator;
import org.apache.gravitino.policy.rego.Not;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.MaskAction;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.MaskBranch;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RowFilter;
import org.apache.gravitino.policy.rego.SessionUser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.functions.IcebergFunctions;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Converts validated restricted Rego programs to closed Iceberg read restrictions. */
public final class IcebergReadRestrictionConverter {

  private IcebergReadRestrictionConverter() {}

  /** Trusted request context used while binding a restriction. */
  public interface BindingContext {

    /**
     * Returns the authenticated effective user name.
     *
     * @return non-empty effective user name
     */
    String sessionUser();

    /**
     * Resolves exact flat membership in the current metalake.
     *
     * <p>An empty result means that the group is unknown or membership could not be resolved. The
     * converter treats that result as an error rather than non-membership.
     *
     * @param group exact group name from the policy expression
     * @return membership when the group and identity snapshot were resolved
     */
    Optional<Boolean> groupMembership(String group);
  }

  /**
   * Binds parsed policy programs to a request context and table schema.
   *
   * <p>The returned restriction contains no symbolic column names or context functions. Constant
   * true row filters are omitted, while constant false filters are retained.
   *
   * @param rowFilter optional effective row-filter program
   * @param columnMasks effective column-mask programs keyed by top-level Iceberg field ID
   * @param schema concrete Iceberg table schema
   * @param context trusted request context
   * @return closed Iceberg read restrictions
   * @throws IllegalArgumentException if any applicable restriction cannot be bound exactly
   */
  public static ReadRestrictions convert(
      Optional<RowFilter> rowFilter,
      Map<Integer, ColumnMask> columnMasks,
      Schema schema,
      BindingContext context) {
    Objects.requireNonNull(rowFilter, "rowFilter cannot be null");
    Objects.requireNonNull(columnMasks, "columnMasks cannot be null");
    Objects.requireNonNull(schema, "schema cannot be null");
    ResolutionContext resolutionContext = new ResolutionContext(context);

    Expression resolvedRowFilter = null;
    if (rowFilter.isPresent()) {
      RowFilter filter = rowFilter.get();
      filter.validate();
      ResolvedPredicate resolved = resolvePredicate(filter.lower(), schema, resolutionContext);
      if (resolved.expression().op() != Expression.Operation.TRUE) {
        resolvedRowFilter = resolved.expression();
      }
    }

    List<Map.Entry<Integer, ColumnMask>> orderedMasks = new ArrayList<>(columnMasks.entrySet());
    for (Map.Entry<Integer, ColumnMask> entry : orderedMasks) {
      if (entry.getKey() == null || entry.getValue() == null) {
        throw new IllegalArgumentException("column mask field ID and program cannot be null");
      }
    }
    orderedMasks.sort(Map.Entry.comparingByKey());

    List<IcebergFunction<?, ?>> projections = new ArrayList<>();
    for (Map.Entry<Integer, ColumnMask> entry : orderedMasks) {
      Types.NestedField field = topLevelField(schema, entry.getKey());
      MaskAction action = resolveMaskAction(entry.getValue(), schema, resolutionContext);
      projections.add(toIcebergFunction(action, field));
    }

    ReadRestrictions restrictions = ReadRestrictions.of(resolvedRowFilter, projections);
    restrictions.validate(Collections.singletonMap(schema.schemaId(), schema));
    return restrictions;
  }

  private static ResolvedPredicate resolvePredicate(
      CanonicalExpression expression, Schema schema, ResolutionContext context) {
    expression.validate();
    if (expression instanceof Literal) {
      Literal literal = (Literal) expression;
      if (literal.literalType() != LiteralType.BOOLEAN) {
        throw new IllegalArgumentException("row-filter literal must be Boolean");
      }
      return constant((Boolean) literal.value());
    }
    if (expression instanceof GroupMembership) {
      return constant(resolveGroup((GroupMembership) expression, context));
    }
    if (expression instanceof Comparison) {
      return resolveComparison((Comparison) expression, schema, context);
    }
    if (expression instanceof Not) {
      ResolvedPredicate child = resolvePredicate(((Not) expression).operand(), schema, context);
      if (child.expression().op() == Expression.Operation.TRUE) {
        return constant(false);
      }
      if (child.expression().op() == Expression.Operation.FALSE) {
        return constant(true);
      }
      return resolved(Expressions.not(child.expression()));
    }
    if (expression instanceof LogicalExpression) {
      return resolveLogical((LogicalExpression) expression, schema, context);
    }
    throw new IllegalArgumentException(
        "Unsupported predicate node after restricted Rego parsing: "
            + expression.getClass().getSimpleName());
  }

  private static ResolvedPredicate resolveLogical(
      LogicalExpression logical, Schema schema, ResolutionContext context) {
    List<ResolvedPredicate> resolvedChildren = new ArrayList<>();
    for (CanonicalExpression operand : logical.operands()) {
      resolvedChildren.add(resolvePredicate(operand, schema, context));
    }

    Map<String, ResolvedPredicate> children = new TreeMap<>();
    boolean absorbingConstant = false;
    for (ResolvedPredicate child : resolvedChildren) {
      if (logical.operator() == LogicalOperator.AND) {
        if (child.expression().op() == Expression.Operation.FALSE) {
          absorbingConstant = true;
          continue;
        }
        if (child.expression().op() == Expression.Operation.TRUE) {
          continue;
        }
      } else {
        if (child.expression().op() == Expression.Operation.TRUE) {
          absorbingConstant = true;
          continue;
        }
        if (child.expression().op() == Expression.Operation.FALSE) {
          continue;
        }
      }
      children.put(child.signature(), child);
    }

    if (absorbingConstant) {
      return constant(logical.operator() == LogicalOperator.OR);
    }

    if (children.isEmpty()) {
      return constant(logical.operator() == LogicalOperator.AND);
    }
    if (children.size() == 1) {
      return children.values().iterator().next();
    }

    Expression combined = null;
    for (ResolvedPredicate child : children.values()) {
      if (combined == null) {
        combined = child.expression();
      } else if (logical.operator() == LogicalOperator.AND) {
        combined = Expressions.and(combined, child.expression());
      } else {
        combined = Expressions.or(combined, child.expression());
      }
    }
    return resolved(Objects.requireNonNull(combined, "logical expression cannot be empty"));
  }

  private static ResolvedPredicate resolveComparison(
      Comparison comparison, Schema schema, ResolutionContext context) {
    CanonicalExpression left = comparison.left();
    CanonicalExpression right = comparison.right();
    ComparisonOperator operator = comparison.operator();

    if (left instanceof SessionUser) {
      return resolveSessionComparison(operator, right, true, schema, context);
    }
    if (right instanceof SessionUser) {
      return resolveSessionComparison(operator, left, false, schema, context);
    }
    if (left instanceof Column) {
      return resolveColumnComparison((Column) left, operator, right, schema, false);
    }
    if (right instanceof Column) {
      if (operator == ComparisonOperator.IN) {
        throw new IllegalArgumentException("in requires the column on the left");
      }
      return resolveColumnComparison((Column) right, invert(operator), left, schema, false);
    }
    throw new IllegalArgumentException("comparison must contain a column or session_user()");
  }

  private static ResolvedPredicate resolveSessionComparison(
      ComparisonOperator operator,
      CanonicalExpression other,
      boolean sessionOnLeft,
      Schema schema,
      ResolutionContext context) {
    String sessionUser = context.sessionUser();
    if (other instanceof Column) {
      ComparisonOperator normalized = sessionOnLeft ? invert(operator) : operator;
      return resolveColumnComparison(
          (Column) other, normalized, SourceValue.sessionUser(sessionUser), schema, true);
    }
    if (other instanceof Literal) {
      Literal literal = (Literal) other;
      if (literal.literalType() != LiteralType.STRING
          || (operator != ComparisonOperator.EQ && operator != ComparisonOperator.NEQ)) {
        throw new IllegalArgumentException(
            "session_user() supports only equality with a string literal or column");
      }
      boolean equal = sessionUser.equals(literal.value());
      return constant(operator == ComparisonOperator.EQ ? equal : !equal);
    }
    if (other instanceof LiteralArray && sessionOnLeft && operator == ComparisonOperator.IN) {
      LiteralArray values = (LiteralArray) other;
      if (values.elementType() != LiteralType.STRING) {
        throw new IllegalArgumentException("session_user() in requires a string array");
      }
      boolean matched = false;
      for (Literal value : values.values()) {
        matched |= sessionUser.equals(value.value());
      }
      return constant(matched);
    }
    throw new IllegalArgumentException(
        "Unsupported session_user() comparison after restricted Rego parsing");
  }

  private static ResolvedPredicate resolveColumnComparison(
      Column column,
      ComparisonOperator operator,
      CanonicalExpression other,
      Schema schema,
      boolean sessionValue) {
    if (other instanceof Literal) {
      return resolveColumnComparison(
          column, operator, SourceValue.literal((Literal) other), schema, sessionValue);
    }
    if (other instanceof LiteralArray) {
      if (operator != ComparisonOperator.IN) {
        throw new IllegalArgumentException("literal arrays are valid only with in");
      }
      return resolveColumnIn(column, (LiteralArray) other, schema);
    }
    throw new IllegalArgumentException("column comparison requires a literal or session_user()");
  }

  private static ResolvedPredicate resolveColumnComparison(
      Column column,
      ComparisonOperator operator,
      SourceValue sourceValue,
      Schema schema,
      boolean sessionValue) {
    Types.NestedField field = topLevelField(schema, column.name());
    if (sessionValue && operator != ComparisonOperator.EQ && operator != ComparisonOperator.NEQ) {
      throw new IllegalArgumentException(
          "session_user() supports only equality with a string column");
    }
    if (sourceValue.literalType() == LiteralType.NULL) {
      if (operator != ComparisonOperator.EQ && operator != ComparisonOperator.NEQ) {
        throw new IllegalArgumentException("null supports only equality comparisons");
      }
      if (field.isRequired()) {
        throw new IllegalArgumentException(
            "null comparison requires an optional field: " + field.name());
      }
      Expression unbound =
          operator == ComparisonOperator.EQ
              ? Expressions.isNull(field.name())
              : Expressions.notNull(field.name());
      return resolved(bind(schema, field, unbound));
    }

    validateOperator(field.type(), operator);
    Object value = typedValue(sourceValue, field.type());
    Expression unbound = predicate(field.name(), operator, value);
    return resolved(bind(schema, field, unbound));
  }

  private static ResolvedPredicate resolveColumnIn(
      Column column, LiteralArray array, Schema schema) {
    Types.NestedField field = topLevelField(schema, column.name());
    validateOperator(field.type(), ComparisonOperator.IN);

    Map<String, Object> values = new TreeMap<>();
    for (Literal literal : array.values()) {
      Object value = typedValue(SourceValue.literal(literal), field.type());
      values.put(canonicalValue(value), value);
    }
    Expression unbound = Expressions.in(field.name(), values.values());
    return resolved(bind(schema, field, unbound));
  }

  private static Expression predicate(String fieldName, ComparisonOperator operator, Object value) {
    switch (operator) {
      case EQ:
        return Expressions.equal(fieldName, value);
      case NEQ:
        return Expressions.notEqual(fieldName, value);
      case LT:
        return Expressions.lessThan(fieldName, value);
      case LTE:
        return Expressions.lessThanOrEqual(fieldName, value);
      case GT:
        return Expressions.greaterThan(fieldName, value);
      case GTE:
        return Expressions.greaterThanOrEqual(fieldName, value);
      default:
        throw new IllegalArgumentException("Unsupported scalar comparison operator: " + operator);
    }
  }

  private static Expression bind(
      Schema schema, Types.NestedField expectedField, Expression expression) {
    Types.NestedField resolvedByIceberg = schema.findField(expectedField.name());
    if (resolvedByIceberg == null || resolvedByIceberg.fieldId() != expectedField.fieldId()) {
      throw new IllegalArgumentException(
          "Column name cannot be bound unambiguously as a top-level field: "
              + expectedField.name());
    }
    Expression bound = Binder.bind(schema.asStruct(), expression, true);
    if (!Binder.boundReferences(schema.asStruct(), bound, true)
        .equals(Collections.singleton(expectedField.fieldId()))) {
      throw new IllegalArgumentException(
          "Bound predicate did not preserve field ID " + expectedField.fieldId());
    }
    return bound;
  }

  private static void validateOperator(Type type, ComparisonOperator operator) {
    switch (type.typeId()) {
      case BOOLEAN:
        if (operator != ComparisonOperator.EQ
            && operator != ComparisonOperator.NEQ
            && operator != ComparisonOperator.IN) {
          throw new IllegalArgumentException("Boolean fields do not support ordering predicates");
        }
        return;
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
      case DATE:
      case TIME:
      case STRING:
        return;
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          throw new IllegalArgumentException(
              "Timestamp-with-timezone fields are not supported by read restrictions");
        }
        return;
      default:
        throw new IllegalArgumentException("Unsupported read-restriction field type: " + type);
    }
  }

  private static Object typedValue(SourceValue source, Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        requireLiteralType(source, LiteralType.BOOLEAN, type);
        return source.value();
      case INTEGER:
        requireLiteralType(source, LiteralType.NUMBER, type);
        try {
          return ((BigDecimal) source.value()).intValueExact();
        } catch (ArithmeticException exception) {
          throw invalidLiteral(source, type, exception);
        }
      case LONG:
        requireLiteralType(source, LiteralType.NUMBER, type);
        try {
          return ((BigDecimal) source.value()).longValueExact();
        } catch (ArithmeticException exception) {
          throw invalidLiteral(source, type, exception);
        }
      case FLOAT:
        requireLiteralType(source, LiteralType.NUMBER, type);
        return exactFloat(source);
      case DOUBLE:
        requireLiteralType(source, LiteralType.NUMBER, type);
        return exactDouble(source);
      case DECIMAL:
        requireLiteralType(source, LiteralType.NUMBER, type);
        return exactDecimal(source, (Types.DecimalType) type);
      case STRING:
        requireLiteralType(source, LiteralType.STRING, type);
        return source.value();
      case DATE:
        requireLiteralType(source, LiteralType.STRING, type);
        validateDate((String) source.value(), type);
        return source.value();
      case TIME:
        requireLiteralType(source, LiteralType.STRING, type);
        validateTime((String) source.value(), type);
        return source.value();
      case TIMESTAMP:
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          throw new IllegalArgumentException(
              "Timestamp-with-timezone fields are not supported by read restrictions");
        }
        requireLiteralType(source, LiteralType.STRING, type);
        validateTimestamp((String) source.value(), type);
        return source.value();
      default:
        throw new IllegalArgumentException("Unsupported read-restriction field type: " + type);
    }
  }

  private static Float exactFloat(SourceValue source) {
    BigDecimal decimal = (BigDecimal) source.value();
    float value = decimal.floatValue();
    if (!Float.isFinite(value) || new BigDecimal((double) value).compareTo(decimal) != 0) {
      throw invalidLiteral(source, Types.FloatType.get(), null);
    }
    return source.negativeZero() ? -0.0F : value;
  }

  private static Double exactDouble(SourceValue source) {
    BigDecimal decimal = (BigDecimal) source.value();
    double value = decimal.doubleValue();
    if (!Double.isFinite(value) || new BigDecimal(value).compareTo(decimal) != 0) {
      throw invalidLiteral(source, Types.DoubleType.get(), null);
    }
    return source.negativeZero() ? -0.0D : value;
  }

  private static BigDecimal exactDecimal(SourceValue source, Types.DecimalType type) {
    BigDecimal value;
    try {
      value = ((BigDecimal) source.value()).setScale(type.scale(), RoundingMode.UNNECESSARY);
    } catch (ArithmeticException exception) {
      throw invalidLiteral(source, type, exception);
    }
    if (value.precision() > type.precision()) {
      throw invalidLiteral(source, type, null);
    }
    return value;
  }

  private static void validateDate(String value, Type type) {
    try {
      LocalDate.parse(value);
    } catch (DateTimeParseException exception) {
      throw invalidLiteral(SourceValue.string(value), type, exception);
    }
  }

  private static void validateTime(String value, Type type) {
    try {
      LocalTime parsed = LocalTime.parse(value);
      if (!parsed.equals(parsed.truncatedTo(ChronoUnit.MICROS))) {
        throw invalidLiteral(SourceValue.string(value), type, null);
      }
    } catch (DateTimeParseException exception) {
      throw invalidLiteral(SourceValue.string(value), type, exception);
    }
  }

  private static void validateTimestamp(String value, Type type) {
    try {
      LocalDateTime parsed = LocalDateTime.parse(value);
      if (!parsed.equals(parsed.truncatedTo(ChronoUnit.MICROS))) {
        throw invalidLiteral(SourceValue.string(value), type, null);
      }
    } catch (DateTimeParseException exception) {
      throw invalidLiteral(SourceValue.string(value), type, exception);
    }
  }

  private static void requireLiteralType(SourceValue source, LiteralType expected, Type fieldType) {
    if (source.literalType() != expected) {
      throw invalidLiteral(source, fieldType, null);
    }
  }

  private static IllegalArgumentException invalidLiteral(
      SourceValue source, Type type, Exception cause) {
    String message =
        String.format(
            "Literal %s cannot be represented exactly as Iceberg %s", source.value(), type);
    return cause == null
        ? new IllegalArgumentException(message)
        : new IllegalArgumentException(message, cause);
  }

  private static boolean resolveGroup(GroupMembership membership, ResolutionContext context) {
    return context.groupMembership(membership.group());
  }

  private static MaskAction resolveMaskAction(
      ColumnMask mask, Schema schema, ResolutionContext context) {
    mask.validate();
    for (MaskBranch branch : mask.branches()) {
      ResolvedPredicate condition = resolvePredicate(branch.condition(), schema, context);
      if (condition.expression().op() != Expression.Operation.TRUE
          && condition.expression().op() != Expression.Operation.FALSE) {
        throw new IllegalArgumentException("column-mask condition did not resolve to a constant");
      }
      if (condition.expression().op() == Expression.Operation.TRUE) {
        return branch.action();
      }
    }
    return mask.fallback();
  }

  private static IcebergFunction<?, ?> toIcebergFunction(
      MaskAction action, Types.NestedField field) {
    IcebergFunction<?, ?> function;
    switch (action) {
      case MASK_ALPHANUM:
        function = IcebergFunctions.maskAlphanum(field.fieldId());
        break;
      case MASK_TO_FIXED_VALUE:
        function = IcebergFunctions.maskToFixedValue(field.fieldId());
        break;
      case REPLACE_WITH_NULL:
        if (field.isRequired()) {
          throw new IllegalArgumentException(
              "replace-with-null is invalid for required field " + field.fieldId());
        }
        function = IcebergFunctions.replaceWithNull(field.fieldId());
        break;
      case SHOW_FIRST_4:
        function = IcebergFunctions.showFirst4(field.fieldId());
        break;
      case SHOW_LAST_4:
        function = IcebergFunctions.showLast4(field.fieldId());
        break;
      case TRUNCATE_TO_YEAR:
        function = IcebergFunctions.truncateToYear(field.fieldId());
        break;
      case TRUNCATE_TO_MONTH:
        function = IcebergFunctions.truncateToMonth(field.fieldId());
        break;
      case SHA_256_GLOBAL:
        function = IcebergFunctions.sha256Global(field.fieldId());
        break;
      case SHA_256_QUERY_LOCAL:
        function = IcebergFunctions.sha256QueryLocal(field.fieldId());
        break;
      default:
        throw new IllegalArgumentException("Unsupported mask action: " + action);
    }
    if (!function.canBind(field.type())) {
      throw new IllegalArgumentException(
          String.format(
              "Mask action %s is incompatible with field %s of type %s",
              action.value(), field.fieldId(), field.type()));
    }
    return function;
  }

  private static Types.NestedField topLevelField(Schema schema, String name) {
    Types.NestedField match = null;
    for (Types.NestedField field : schema.columns()) {
      if (field.name().equals(name)) {
        if (match != null) {
          throw new IllegalArgumentException("Ambiguous top-level column: " + name);
        }
        match = field;
      }
    }
    if (match == null) {
      throw new IllegalArgumentException("Cannot find top-level column: " + name);
    }
    return match;
  }

  private static Types.NestedField topLevelField(Schema schema, int fieldId) {
    for (Types.NestedField field : schema.columns()) {
      if (field.fieldId() == fieldId) {
        return field;
      }
    }
    throw new IllegalArgumentException("Cannot find top-level field ID: " + fieldId);
  }

  private static ComparisonOperator invert(ComparisonOperator operator) {
    switch (operator) {
      case EQ:
      case NEQ:
        return operator;
      case LT:
        return ComparisonOperator.GT;
      case LTE:
        return ComparisonOperator.GTE;
      case GT:
        return ComparisonOperator.LT;
      case GTE:
        return ComparisonOperator.LTE;
      default:
        throw new IllegalArgumentException("Cannot invert comparison operator: " + operator);
    }
  }

  private static String canonicalValue(Object value) {
    if (value instanceof BigDecimal) {
      return ((BigDecimal) value).toPlainString();
    }
    if (value instanceof Float) {
      return Integer.toUnsignedString(Float.floatToIntBits((Float) value));
    }
    if (value instanceof Double) {
      return Long.toUnsignedString(Double.doubleToLongBits((Double) value));
    }
    return String.valueOf(value);
  }

  private static ResolvedPredicate constant(boolean value) {
    return resolved(value ? Expressions.alwaysTrue() : Expressions.alwaysFalse());
  }

  private static ResolvedPredicate resolved(Expression expression) {
    return new ResolvedPredicate(expression, ExpressionParser.toJson(expression));
  }

  private static final class ResolvedPredicate {
    private final Expression expression;
    private final String signature;

    private ResolvedPredicate(Expression expression, String signature) {
      this.expression = expression;
      this.signature = signature;
    }

    private Expression expression() {
      return expression;
    }

    private String signature() {
      return signature;
    }
  }

  private static final class SourceValue {
    private final LiteralType literalType;
    private final Object value;
    private final boolean negativeZero;

    private SourceValue(LiteralType literalType, Object value, boolean negativeZero) {
      this.literalType = literalType;
      this.value = value;
      this.negativeZero = negativeZero;
    }

    private static SourceValue literal(Literal literal) {
      return new SourceValue(literal.literalType(), literal.value(), literal.negativeZero());
    }

    private static SourceValue sessionUser(String value) {
      return string(value);
    }

    private static SourceValue string(String value) {
      return new SourceValue(LiteralType.STRING, value, false);
    }

    private LiteralType literalType() {
      return literalType;
    }

    private Object value() {
      return value;
    }

    private boolean negativeZero() {
      return negativeZero;
    }
  }

  private static final class ResolutionContext {
    private final BindingContext delegate;
    private final String sessionUser;
    private final Map<String, Boolean> groupMemberships = new HashMap<>();

    private ResolutionContext(BindingContext delegate) {
      this.delegate = Objects.requireNonNull(delegate, "context cannot be null");
      this.sessionUser = delegate.sessionUser();
      if (sessionUser == null || sessionUser.isEmpty()) {
        throw new IllegalArgumentException("authenticated session user cannot be null or empty");
      }
    }

    private String sessionUser() {
      return sessionUser;
    }

    private boolean groupMembership(String group) {
      if (groupMemberships.containsKey(group)) {
        return groupMemberships.get(group);
      }
      Optional<Boolean> result = delegate.groupMembership(group);
      if (result == null || !result.isPresent()) {
        throw new IllegalArgumentException(
            "Cannot resolve group membership for the applicable read restriction");
      }
      groupMemberships.put(group, result.get());
      return result.get();
    }
  }
}
