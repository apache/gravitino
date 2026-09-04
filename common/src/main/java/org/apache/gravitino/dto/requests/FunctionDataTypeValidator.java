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
package org.apache.gravitino.dto.requests;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.dto.function.FunctionColumnDTO;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
import org.apache.gravitino.dto.function.FunctionParamDTO;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

final class FunctionDataTypeValidator {

  private FunctionDataTypeValidator() {}

  static void validateDefinition(FunctionDefinitionDTO definition, String fieldPath) {
    Preconditions.checkArgument(
        definition != null, "\"%s\" field is required and cannot be null", fieldPath);

    FunctionParamDTO[] parameters = definition.getParameters();
    if (parameters != null) {
      for (int i = 0; i < parameters.length; i++) {
        String parameterPath = fieldPath + ".parameters[" + i + "]";
        Preconditions.checkArgument(
            parameters[i] != null, "\"%s\" field is required and cannot be null", parameterPath);
        validateDataType(parameters[i].getDataType(), parameterPath + ".dataType");
      }
    }

    if (definition.getReturnType() != null) {
      validateDataType(definition.getReturnType(), fieldPath + ".returnType");
    }

    FunctionColumnDTO[] returnColumns = definition.getReturnColumns();
    if (returnColumns != null) {
      for (int i = 0; i < returnColumns.length; i++) {
        String returnColumnPath = fieldPath + ".returnColumns[" + i + "]";
        Preconditions.checkArgument(
            returnColumns[i] != null,
            "\"%s\" field is required and cannot be null",
            returnColumnPath);
        validateDataType(returnColumns[i].getDataType(), returnColumnPath + ".dataType");
      }
    }
  }

  private static void validateDataType(Type dataType, String fieldPath) {
    Preconditions.checkArgument(
        dataType != null, "\"%s\" field is required and cannot be null", fieldPath);
    Preconditions.checkArgument(
        !(dataType instanceof Types.UnparsedType),
        "\"%s\" must be a Gravitino-recognized data type or an explicit ExternalType; "
            + "UnparsedType is not allowed for new function definitions",
        fieldPath);

    if (dataType instanceof Types.ExternalType) {
      Types.ExternalType externalType = (Types.ExternalType) dataType;
      Preconditions.checkArgument(
          StringUtils.isNotBlank(externalType.catalogString()),
          "\"%s.catalogString\" field is required and cannot be empty",
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
        validateDataType(fields[i].type(), fieldPath + ".fields[" + i + "].type");
      }
      return;
    }

    if (dataType instanceof Types.ListType) {
      validateDataType(((Types.ListType) dataType).elementType(), fieldPath + ".elementType");
      return;
    }

    if (dataType instanceof Types.MapType) {
      Types.MapType mapType = (Types.MapType) dataType;
      validateDataType(mapType.keyType(), fieldPath + ".keyType");
      validateDataType(mapType.valueType(), fieldPath + ".valueType");
      return;
    }

    if (dataType instanceof Types.UnionType) {
      Type[] types = ((Types.UnionType) dataType).types();
      Preconditions.checkArgument(
          types != null, "\"%s.types\" field is required and cannot be null", fieldPath);
      for (int i = 0; i < types.length; i++) {
        validateDataType(types[i], fieldPath + ".types[" + i + "]");
      }
      return;
    }

    Preconditions.checkArgument(
        dataType instanceof Type.PrimitiveType || dataType instanceof Types.NullType,
        "\"%s\" must be a Gravitino-recognized data type or an explicit ExternalType",
        fieldPath);
  }
}
