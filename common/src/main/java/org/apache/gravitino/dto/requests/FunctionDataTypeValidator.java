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
import org.apache.gravitino.dto.function.FunctionColumnDTO;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
import org.apache.gravitino.dto.function.FunctionParamDTO;
import org.apache.gravitino.utils.WritableDataTypeValidator;

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
        WritableDataTypeValidator.validate(
            parameters[i].getDataType(), parameterPath + ".dataType");
        WritableDataTypeValidator.validateExpression(
            parameters[i].defaultValue(), parameterPath + ".defaultValue");
      }
    }

    if (definition.getReturnType() != null) {
      WritableDataTypeValidator.validate(definition.getReturnType(), fieldPath + ".returnType");
    }

    FunctionColumnDTO[] returnColumns = definition.getReturnColumns();
    if (returnColumns != null) {
      for (int i = 0; i < returnColumns.length; i++) {
        String returnColumnPath = fieldPath + ".returnColumns[" + i + "]";
        Preconditions.checkArgument(
            returnColumns[i] != null,
            "\"%s\" field is required and cannot be null",
            returnColumnPath);
        WritableDataTypeValidator.validate(
            returnColumns[i].getDataType(), returnColumnPath + ".dataType");
      }
    }
  }
}
