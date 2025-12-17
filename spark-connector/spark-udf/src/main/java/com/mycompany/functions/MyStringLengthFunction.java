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
package com.mycompany.functions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

public class MyStringLengthFunction
    implements UnboundFunction, ScalarFunction<Integer>, BoundFunction {

  public static final String NAME = "my_str_len";

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType == null || inputType.fields().length != 1) {
      throw new IllegalArgumentException("my_str_len requires exactly one STRING argument");
    }
    if (!DataTypes.StringType.sameType(inputType.fields()[0].dataType())) {
      throw new IllegalArgumentException("my_str_len requires a STRING argument");
    }
    return this;
  }

  @Override
  public String description() {
    return "Returns the length of a string.";
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public DataType[] inputTypes() {
    return new DataType[] {DataTypes.StringType};
  }

  @Override
  public DataType resultType() {
    return DataTypes.IntegerType;
  }

  @Override
  public boolean isResultNullable() {
    return true;
  }

  @Override
  public Integer produceResult(InternalRow input) {
    if (input.isNullAt(0)) {
      return null;
    }
    UTF8String utf8String = input.getUTF8String(0);
    return utf8String == null ? null : utf8String.numChars();
  }
}
