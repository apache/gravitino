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
package com.datastrato.gravitino.catalog.lakehouse.paimon.utils;

import com.apache.gravitino.rel.Column;
import com.apache.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.catalog.lakehouse.paimon.ops.PaimonCatalogOps;
import com.google.common.base.Preconditions;

/** Utilities of {@link PaimonCatalogOps} to support table operation. */
public class TableOpsUtils {

  public static void checkColumnCapability(
      String fieldName, Expression defaultValue, boolean autoIncrement) {
    checkColumnDefaultValue(fieldName, defaultValue);
    checkColumnAutoIncrement(fieldName, autoIncrement);
  }

  private static void checkColumnDefaultValue(String fieldName, Expression defaultValue) {
    Preconditions.checkArgument(
        defaultValue.equals(Column.DEFAULT_VALUE_NOT_SET),
        String.format(
            "Paimon set column default value through table properties instead of column info. Illegal column: %s.",
            fieldName));
  }

  private static void checkColumnAutoIncrement(String fieldName, boolean autoIncrement) {
    Preconditions.checkArgument(
        !autoIncrement,
        String.format(
            "Paimon does not support auto increment column. Illegal column: %s.", fieldName));
  }
}
