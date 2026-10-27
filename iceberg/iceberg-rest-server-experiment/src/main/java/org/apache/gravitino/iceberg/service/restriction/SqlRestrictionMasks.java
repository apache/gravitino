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
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.iceberg.service.restriction.SqlRestrictionPredicates.Truth;
import org.apache.gravitino.policy.sql.CaseBranch;
import org.apache.gravitino.policy.sql.CaseExpression;
import org.apache.gravitino.policy.sql.FunctionCall;
import org.apache.gravitino.policy.sql.InputReference;
import org.apache.gravitino.policy.sql.Literal;
import org.apache.gravitino.policy.sql.LiteralType;
import org.apache.gravitino.policy.sql.SqlExpression;
import org.apache.gravitino.policy.sql.SqlFunction;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.functions.IcebergFunction;
import org.apache.iceberg.functions.IcebergFunctions;
import org.apache.iceberg.types.Types;

/** Converts selected scalar SQL mask results to one supported Iceberg projection per field. */
final class SqlRestrictionMasks {
  private final Types.NestedField field;
  private final SqlRestrictionPredicates predicates;

  SqlRestrictionMasks(Types.NestedField field, SqlRestrictionPredicates predicates) {
    this.field = field;
    this.predicates = predicates;
  }

  @Nullable
  IcebergFunction<?, ?> resolve(SqlExpression body) {
    if (field.isRequired() && nullable(body)) {
      throw new IllegalArgumentException(
          "Nullable SQL mask result is invalid for required field " + field.fieldId());
    }
    return selected(body);
  }

  @Nullable
  private IcebergFunction<?, ?> selected(SqlExpression body) {
    if (body instanceof InputReference) {
      return null;
    }
    if (body instanceof CaseExpression) {
      CaseExpression choice = (CaseExpression) body;
      for (CaseBranch branch : choice.branches()) {
        Truth condition =
            choice.selector() == null
                ? predicates.resolve(branch.condition())
                : equality(choice.selector(), branch.condition());
        if (!condition.constant()) {
          throw SqlRestrictionBinding.unsupported("row-dependent mask selection");
        }
        if (condition.yes.op() == Expression.Operation.TRUE) {
          return selected(branch.result());
        }
      }
      return choice.fallback() == null
          ? checked(IcebergFunctions.replaceWithNull(field.fieldId()))
          : selected(choice.fallback());
    }
    if (body instanceof FunctionCall) {
      FunctionCall call = (FunctionCall) body;
      if (call.arguments().size() == 1 && call.arguments().get(0) instanceof InputReference) {
        return checked(mask(call.function()));
      }
    }
    Literal literal = predicates.constantScalar(body);
    Object value = SqlRestrictionLiterals.value(literal, field.type());
    if (value == null) {
      return checked(IcebergFunctions.replaceWithNull(field.fieldId()));
    }
    IcebergFunction<?, ?> fixed = checked(IcebergFunctions.maskToFixedValue(field.fieldId()));
    Object fixedValue = fixed.bind(field.type()).apply(null);
    boolean same =
        value instanceof BigDecimal && fixedValue instanceof BigDecimal
            ? ((BigDecimal) value).compareTo((BigDecimal) fixedValue) == 0
            : Objects.equals(value, fixedValue);
    if (!same) {
      throw SqlRestrictionBinding.unsupported("an arbitrary constant mask value");
    }
    return fixed;
  }

  private Truth equality(SqlExpression selector, SqlExpression match) {
    return predicates.equal(selector, match);
  }

  private IcebergFunction<?, ?> mask(SqlFunction function) {
    switch (function) {
      case MASK_ALPHANUM:
        return IcebergFunctions.maskAlphanum(field.fieldId());
      case SHOW_FIRST_4:
        return IcebergFunctions.showFirst4(field.fieldId());
      case SHOW_LAST_4:
        return IcebergFunctions.showLast4(field.fieldId());
      case MASK_TO_FIXED_VALUE:
        return IcebergFunctions.maskToFixedValue(field.fieldId());
      case REPLACE_WITH_NULL:
        return IcebergFunctions.replaceWithNull(field.fieldId());
      case TRUNCATE_TO_YEAR:
        return IcebergFunctions.truncateToYear(field.fieldId());
      case TRUNCATE_TO_MONTH:
        return IcebergFunctions.truncateToMonth(field.fieldId());
      case SHA256_GLOBAL:
        return IcebergFunctions.sha256Global(field.fieldId());
      case SHA256_QUERY_LOCAL:
        return IcebergFunctions.sha256QueryLocal(field.fieldId());
      default:
        throw SqlRestrictionBinding.unsupported("the selected mask function " + function);
    }
  }

  private IcebergFunction<?, ?> checked(IcebergFunction<?, ?> function) {
    if (function.name().equals("replace-with-null") && field.isRequired()) {
      throw new IllegalArgumentException(
          "NULL mask is invalid for required field " + field.fieldId());
    }
    if (!function.canBind(field.type())) {
      throw new IllegalArgumentException(
          "Mask " + function.name() + " is incompatible with selected field type " + field.type());
    }
    return function;
  }

  private boolean nullable(SqlExpression expression) {
    if (expression instanceof Literal) {
      return ((Literal) expression).literalType() == LiteralType.NULL;
    }
    if (expression instanceof InputReference) {
      return field.isOptional();
    }
    if (expression instanceof CaseExpression) {
      CaseExpression choice = (CaseExpression) expression;
      if (choice.fallback() == null || nullable(choice.fallback())) {
        return true;
      }
      for (CaseBranch branch : choice.branches()) {
        if (nullable(branch.result())) {
          return true;
        }
      }
      return false;
    }
    if (expression instanceof FunctionCall) {
      FunctionCall call = (FunctionCall) expression;
      switch (call.function()) {
        case CURRENT_USER:
        case IS_GROUP_MEMBER:
        case MASK_TO_FIXED_VALUE:
          return false;
        case USER_ATTRIBUTE:
        case REPLACE_WITH_NULL:
          return true;
        default:
          for (SqlExpression argument : call.arguments()) {
            if (nullable(argument)) {
              return true;
            }
          }
          return false;
      }
    }
    return true;
  }
}
