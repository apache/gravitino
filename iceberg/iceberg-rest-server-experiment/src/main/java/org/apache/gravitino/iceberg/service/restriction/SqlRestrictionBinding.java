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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.gravitino.iceberg.service.restriction.IcebergReadRestrictionConverter.BindingContext;
import org.apache.gravitino.policy.sql.FunctionCall;
import org.apache.gravitino.policy.sql.InputReference;
import org.apache.gravitino.policy.sql.Literal;
import org.apache.gravitino.policy.sql.LiteralType;
import org.apache.gravitino.policy.sql.SqlDefinition;
import org.apache.gravitino.policy.sql.SqlExpression;
import org.apache.gravitino.policy.sql.SqlFunction;
import org.apache.gravitino.policy.sql.SqlType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Binds all declared inputs before evaluating context-dependent SQL branches. */
final class SqlRestrictionBinding {
  private final Schema schema;
  private final Context context;
  private final Map<String, Types.NestedField> inputs = new LinkedHashMap<>();

  SqlRestrictionBinding(
      SqlDefinition definition,
      Schema schema,
      Context context,
      @Nullable Types.NestedField maskField) {
    this.schema = schema;
    this.context = context;
    if (definition.isColumnMask() != (maskField != null)) {
      throw new IllegalArgumentException("SQL definition does not match restriction target");
    }
    for (Map.Entry<String, SqlType> parameter : definition.parameters().entrySet()) {
      Types.NestedField field =
          maskField == null
              ? findField(
                  schema, parameter.getKey(), definition.isQuotedParameter(parameter.getKey()))
              : maskField;
      if (!SqlRestrictionLiterals.type(parameter.getValue()).equals(field.type())) {
        throw new IllegalArgumentException(
            "Declared SQL type does not match column " + field.name());
      }
      inputs.put(parameter.getKey(), field);
    }
    if (maskField != null
        && !SqlRestrictionLiterals.type(definition.resultType()).equals(maskField.type())) {
      throw new IllegalArgumentException("Mask result type does not match selected column");
    }
    if (maskField == null && !definition.resultType().equals(SqlType.BOOLEAN)) {
      throw new IllegalArgumentException("Row filter must return BOOLEAN");
    }
  }

  Term term(SqlExpression expression) {
    if (expression instanceof InputReference) {
      Types.NestedField field = inputs.get(((InputReference) expression).name());
      if (field == null) {
        throw new IllegalArgumentException("Unbound SQL parameter");
      }
      return new Term(field, Expressions.ref(field.name()), field.type(), false);
    }
    if (expression instanceof FunctionCall) {
      FunctionCall call = (FunctionCall) expression;
      if (call.function() == SqlFunction.YEAR
          && call.arguments().get(0) instanceof InputReference) {
        Term input = term(call.arguments().get(0));
        return new Term(
            input.field, Expressions.year(input.field.name()), Types.IntegerType.get(), true);
      }
    }
    throw unsupported("the scalar input or date-part function");
  }

  Literal constant(SqlExpression expression) {
    if (expression instanceof Literal) {
      return (Literal) expression;
    }
    if (!(expression instanceof FunctionCall)) {
      throw unsupported("a row-dependent scalar value");
    }
    FunctionCall call = (FunctionCall) expression;
    switch (call.function()) {
      case CURRENT_USER:
        return new Literal(LiteralType.STRING, context.user, SqlType.STRING);
      case IS_GROUP_MEMBER:
        return bool(context.group(((Literal) call.arguments().get(0)).value()));
      case YEAR:
      case MONTH:
      case DAY:
        Literal input = constant(call.arguments().get(0));
        if (input.literalType() == LiteralType.NULL) {
          return nullLiteral();
        }
        if (input.type() == null || !input.type().temporal()) {
          throw new IllegalArgumentException("Date part requires a typed temporal value");
        }
        SqlRestrictionLiterals.value(input, SqlRestrictionLiterals.type(input.type()));
        LocalDate date;
        if (input.type().name().equals("DATE")) {
          date = LocalDate.parse(input.value());
        } else if (input.type().withTimeZone()) {
          date =
              OffsetDateTime.parse(input.value().replace(' ', 'T'))
                  .withOffsetSameInstant(ZoneOffset.UTC)
                  .toLocalDate();
        } else {
          date = LocalDateTime.parse(input.value().replace(' ', 'T')).toLocalDate();
        }
        int part =
            call.function() == SqlFunction.YEAR
                ? date.getYear()
                : call.function() == SqlFunction.MONTH
                    ? date.getMonthValue()
                    : date.getDayOfMonth();
        return new Literal(LiteralType.NUMBER, Integer.toString(part), SqlType.INTEGER);
      default:
        throw unsupported("the scalar function " + call.function());
    }
  }

  Expression bind(Term term, Expression predicate) {
    Types.NestedField byName = schema.findField(term.field.name());
    if (byName == null || byName.fieldId() != term.field.fieldId()) {
      throw new IllegalArgumentException(
          "Cannot bind top-level field unambiguously: " + term.field.name());
    }
    Expression result = Binder.bind(schema.asStruct(), predicate, true);
    if (result.op() != Expression.Operation.TRUE
        && result.op() != Expression.Operation.FALSE
        && !Binder.boundReferences(schema.asStruct(), result, true)
            .equals(Collections.singleton(term.field.fieldId()))) {
      throw new IllegalArgumentException("Bound expression did not preserve the selected field ID");
    }
    return result;
  }

  static Types.NestedField field(Schema schema, int id) {
    for (Types.NestedField field : schema.columns()) {
      if (field.fieldId() == id) {
        return field;
      }
    }
    throw new IllegalArgumentException("Cannot find top-level field ID: " + id);
  }

  static IllegalArgumentException unsupported(String feature) {
    return new IllegalArgumentException("Iceberg read restrictions cannot represent " + feature);
  }

  static Literal bool(boolean value) {
    return new Literal(LiteralType.BOOLEAN, Boolean.toString(value), SqlType.BOOLEAN);
  }

  static Literal nullLiteral() {
    return new Literal(LiteralType.NULL, "", null);
  }

  private static Types.NestedField findField(Schema schema, String name, boolean quoted) {
    Types.NestedField match = null;
    for (Types.NestedField field : schema.columns()) {
      boolean matches =
          quoted ? field.name().equals(name) : field.name().toUpperCase(Locale.ROOT).equals(name);
      if (matches) {
        if (match != null) {
          throw new IllegalArgumentException("Ambiguous top-level SQL parameter: " + name);
        }
        match = field;
      }
    }
    if (match == null) {
      throw new IllegalArgumentException("Cannot find top-level SQL parameter: " + name);
    }
    return match;
  }

  static final class Term {
    final Types.NestedField field;
    final UnboundTerm<Object> expression;
    final Type type;
    private final boolean calendarYear;

    Term(Types.NestedField field, UnboundTerm<Object> expression, Type type, boolean calendarYear) {
      this.field = field;
      this.expression = expression;
      this.type = type;
      this.calendarYear = calendarYear;
    }

    Object value(Literal literal) {
      Object value = SqlRestrictionLiterals.value(literal, type);
      return calendarYear && value != null ? Math.subtractExact((Integer) value, 1970) : value;
    }
  }

  static final class Context {
    private final BindingContext delegate;
    private final String user;
    private final Map<String, Boolean> groups = new HashMap<>();

    Context(BindingContext delegate) {
      this.delegate = Objects.requireNonNull(delegate, "context cannot be null");
      this.user = delegate.sessionUser();
      if (user == null || user.isEmpty()) {
        throw new IllegalArgumentException("Authenticated user cannot be null or empty");
      }
    }

    boolean group(String name) {
      return groups.computeIfAbsent(
          name,
          key -> {
            Optional<Boolean> result = delegate.groupMembership(key);
            if (result == null || !result.isPresent()) {
              throw new IllegalArgumentException(
                  "Cannot resolve group membership for read restriction");
            }
            return result.get();
          });
    }
  }
}
