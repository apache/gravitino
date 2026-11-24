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
package org.apache.gravitino.utils;

import com.google.common.base.Preconditions;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/**
 * Validates data types in newly written metadata. Read and import paths, and selectors identifying
 * existing metadata, must retain support for {@link Types.UnparsedType}.
 *
 * <p>This checks the type representation, not whether a connector supports the type or whether a
 * literal value matches its declared type.
 */
public final class WritableDataTypeValidator {

  private WritableDataTypeValidator() {}

  /**
   * Validates a type, including all nested types.
   *
   * @param dataType The type to validate.
   * @param fieldPath The request field path used in validation errors.
   * @throws IllegalArgumentException If the type is unresolved or an external type is blank.
   */
  public static void validate(Type dataType, String fieldPath) {
    Preconditions.checkArgument(
        dataType != null, "\"%s\" field is required and cannot be null", fieldPath);
    Preconditions.checkArgument(
        !(dataType instanceof Types.UnparsedType),
        "\"%s\" must be a Gravitino-recognized data type or an explicit ExternalType; "
            + "UnparsedType is not allowed for new metadata",
        fieldPath);

    if (dataType instanceof Types.ExternalType) {
      Types.ExternalType externalType = (Types.ExternalType) dataType;
      Preconditions.checkArgument(
          StringUtils.isNotBlank(externalType.catalogString()),
          "\"%s.catalogString\" field is required and cannot be blank",
          fieldPath);
      return;
    }

    if (dataType instanceof Types.StructType) {
      Types.StructType.Field[] fields = ((Types.StructType) dataType).fields();
      Preconditions.checkArgument(
          fields != null, "\"%s.fields\" field is required and cannot be null", fieldPath);
      for (int i = 0; i < fields.length; i++) {
        Preconditions.checkArgument(
            fields[i] != null,
            "\"%s.fields[%s]\" field is required and cannot be null",
            fieldPath,
            i);
        validate(fields[i].type(), fieldPath + ".fields[" + i + "].type");
      }
      return;
    }

    if (dataType instanceof Types.ListType) {
      validate(((Types.ListType) dataType).elementType(), fieldPath + ".elementType");
      return;
    }

    if (dataType instanceof Types.MapType) {
      Types.MapType mapType = (Types.MapType) dataType;
      validate(mapType.keyType(), fieldPath + ".keyType");
      validate(mapType.valueType(), fieldPath + ".valueType");
      return;
    }

    if (dataType instanceof Types.UnionType) {
      Type[] types = ((Types.UnionType) dataType).types();
      Preconditions.checkArgument(
          types != null, "\"%s.types\" field is required and cannot be null", fieldPath);
      for (int i = 0; i < types.length; i++) {
        validate(types[i], fieldPath + ".types[" + i + "]");
      }
      return;
    }

    Preconditions.checkArgument(
        dataType instanceof Type.PrimitiveType || dataType instanceof Types.NullType,
        "\"%s\" must be a Gravitino-recognized data type or an explicit ExternalType",
        fieldPath);
  }

  /**
   * Validates literal types recursively within an expression and any partition assignments.
   *
   * @param expression The expression to validate, or null when absent.
   * @param fieldPath The request field path used in validation errors.
   * @throws IllegalArgumentException If a literal contains a non-writable type, a traversal array
   *     is null, or a partition assignment is null or unsupported.
   */
  public static void validateExpression(@Nullable Expression expression, String fieldPath) {
    if (expression == null) {
      return;
    }
    if (expression instanceof Literal) {
      validate(((Literal<?>) expression).dataType(), fieldPath + ".dataType");
    }
    if (expression instanceof Transform) {
      Partition[] assignments = ((Transform) expression).assignments();
      Preconditions.checkArgument(
          assignments != null,
          "\"%s.assignments\" field is required and cannot be null",
          fieldPath);
      for (int i = 0; i < assignments.length; i++) {
        validatePartition(assignments[i], fieldPath + ".assignments[" + i + "]");
      }
    }
    Expression[] children = expression.children();
    String childField =
        expression instanceof FunctionExpression || expression instanceof Transform
            ? ".funcArgs"
            : ".children";
    Preconditions.checkArgument(
        children != null, "\"%s\" field is required and cannot be null", fieldPath + childField);
    for (int i = 0; i < children.length; i++) {
      validateExpression(children[i], fieldPath + childField + "[" + i + "]");
    }
  }

  /**
   * Validates the literal types in a newly added partition.
   *
   * @param partition The partition to validate.
   * @param fieldPath The request field path used in validation errors.
   * @throws IllegalArgumentException If a literal contains a non-writable type, a traversal array
   *     is null, or the partition is null or unsupported.
   */
  public static void validatePartition(Partition partition, String fieldPath) {
    Preconditions.checkArgument(
        partition != null, "\"%s\" field is required and cannot be null", fieldPath);
    if (partition instanceof IdentityPartition) {
      Literal<?>[] values = ((IdentityPartition) partition).values();
      Preconditions.checkArgument(
          values != null, "\"%s.values\" field is required and cannot be null", fieldPath);
      for (int i = 0; i < values.length; i++) {
        validateExpression(values[i], fieldPath + ".values[" + i + "]");
      }
    } else if (partition instanceof ListPartition) {
      Literal<?>[][] lists = ((ListPartition) partition).lists();
      Preconditions.checkArgument(
          lists != null, "\"%s.lists\" field is required and cannot be null", fieldPath);
      for (int i = 0; i < lists.length; i++) {
        Preconditions.checkArgument(
            lists[i] != null,
            "\"%s.lists[%s]\" field is required and cannot be null",
            fieldPath,
            i);
        for (int j = 0; j < lists[i].length; j++) {
          validateExpression(lists[i][j], fieldPath + ".lists[" + i + "][" + j + "]");
        }
      }
    } else if (partition instanceof RangePartition) {
      RangePartition range = (RangePartition) partition;
      validateExpression(range.lower(), fieldPath + ".lower");
      validateExpression(range.upper(), fieldPath + ".upper");
    } else {
      throw new IllegalArgumentException(
          String.format(
              "\"%s\" must be an IdentityPartition, ListPartition or RangePartition", fieldPath));
    }
  }

  /**
   * Validates the types and defaults of newly supplied table or view columns.
   *
   * @param columns The new columns.
   * @param fieldPath The request field path of the columns array.
   * @throws IllegalArgumentException If a new type or literal type is not writable, or the columns
   *     array or a column is null.
   */
  public static void validateColumns(Column[] columns, String fieldPath) {
    Preconditions.checkArgument(
        columns != null, "\"%s\" field is required and cannot be null", fieldPath);
    for (int i = 0; i < columns.length; i++) {
      String columnPath = fieldPath + "[" + i + "]";
      Preconditions.checkArgument(
          columns[i] != null, "\"%s\" field is required and cannot be null", columnPath);
      validate(columns[i].dataType(), columnPath + ".type");
      validateExpression(columns[i].defaultValue(), columnPath + ".defaultValue");
    }
  }

  /**
   * Validates types and expressions in a table creation request.
   *
   * @param columns The new columns.
   * @param partitioning The partitioning, or null when absent.
   * @param distribution The distribution, or null when absent.
   * @param sortOrders The sort orders, or null when absent.
   * @throws IllegalArgumentException If a new type is not writable, or a required array, column or
   *     sort order is null.
   */
  public static void validateTable(
      Column[] columns,
      @Nullable Transform[] partitioning,
      @Nullable Distribution distribution,
      @Nullable SortOrder[] sortOrders) {
    validateColumns(columns, "columns");
    if (partitioning != null) {
      for (int i = 0; i < partitioning.length; i++) {
        validateExpression(partitioning[i], "partitioning[" + i + "]");
      }
    }
    if (distribution != null) {
      Expression[] expressions = distribution.expressions();
      Preconditions.checkArgument(
          expressions != null, "\"distribution.funcArgs\" field is required and cannot be null");
      for (int i = 0; i < expressions.length; i++) {
        validateExpression(expressions[i], "distribution.funcArgs[" + i + "]");
      }
    }
    if (sortOrders != null) {
      for (int i = 0; i < sortOrders.length; i++) {
        Preconditions.checkArgument(
            sortOrders[i] != null, "\"sortOrders[%s]\" field is required and cannot be null", i);
        validateExpression(sortOrders[i].expression(), "sortOrders[" + i + "].sortTerm");
      }
    }
  }

  /**
   * Validates only the types or defaults introduced by a table change.
   *
   * @param change The table change.
   * @param fieldPath The request field path used in validation errors.
   * @throws IllegalArgumentException If a new type is not writable.
   */
  public static void validateTableChange(TableChange change, String fieldPath) {
    if (change instanceof TableChange.AddColumn) {
      TableChange.AddColumn add = (TableChange.AddColumn) change;
      validate(add.getDataType(), fieldPath + ".type");
      validateExpression(add.getDefaultValue(), fieldPath + ".defaultValue");
    } else if (change instanceof TableChange.UpdateColumnType) {
      validate(((TableChange.UpdateColumnType) change).getNewDataType(), fieldPath + ".newType");
    } else if (change instanceof TableChange.UpdateColumnDefaultValue) {
      validateExpression(
          ((TableChange.UpdateColumnDefaultValue) change).getNewDefaultValue(),
          fieldPath + ".newDefaultValue");
    }
  }

  /**
   * Validates a newly registered function definition, including parameter defaults.
   *
   * @param definition The new definition.
   * @param fieldPath The request field path used in validation errors.
   * @throws IllegalArgumentException If a new type is not writable, or the definition, a required
   *     array or an array element is null.
   */
  public static void validateDefinition(FunctionDefinition definition, String fieldPath) {
    Preconditions.checkArgument(
        definition != null, "\"%s\" field is required and cannot be null", fieldPath);
    FunctionParam[] parameters = definition.parameters();
    Preconditions.checkArgument(
        parameters != null, "\"%s.parameters\" field is required and cannot be null", fieldPath);
    for (int i = 0; i < parameters.length; i++) {
      String parameterPath = fieldPath + ".parameters[" + i + "]";
      Preconditions.checkArgument(
          parameters[i] != null, "\"%s\" field is required and cannot be null", parameterPath);
      validate(parameters[i].dataType(), parameterPath + ".dataType");
      validateExpression(parameters[i].defaultValue(), parameterPath + ".defaultValue");
    }
    if (definition.returnType() != null) {
      validate(definition.returnType(), fieldPath + ".returnType");
    }
    FunctionColumn[] columns = definition.returnColumns();
    Preconditions.checkArgument(
        columns != null, "\"%s.returnColumns\" field is required and cannot be null", fieldPath);
    for (int i = 0; i < columns.length; i++) {
      String columnPath = fieldPath + ".returnColumns[" + i + "]";
      Preconditions.checkArgument(
          columns[i] != null, "\"%s\" field is required and cannot be null", columnPath);
      validate(columns[i].dataType(), columnPath + ".dataType");
    }
  }
}
