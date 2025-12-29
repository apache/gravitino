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
package org.apache.gravitino.spark.connector.functions;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A test UDF implementation that returns the length of a string. This is used for testing
 * Gravitino's UDF support in Spark.
 */
public class StringLengthFunction implements UnboundFunction {

  @Override
  public String name() {
    return "my_string_length";
  }

  @Override
  public BoundFunction bind(StructType inputType) {
    if (inputType.fields().length != 1) {
      throw new UnsupportedOperationException("my_string_length expects exactly one argument");
    }
    if (!inputType.fields()[0].dataType().equals(DataTypes.StringType)) {
      throw new UnsupportedOperationException(
          "my_string_length expects a string argument, got: " + inputType.fields()[0].dataType());
    }
    return new BoundStringLengthFunction();
  }

  @Override
  public String description() {
    return "Returns the length of a string";
  }

  private static class BoundStringLengthFunction implements ScalarFunction<Integer> {
    @Override
    public String name() {
      return "my_string_length";
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
    public String canonicalName() {
      return "gravitino.my_string_length";
    }

    @Override
    public Integer produceResult(InternalRow input) {
      if (input.isNullAt(0)) {
        return null;
      }
      UTF8String str = input.getUTF8String(0);
      return str.numChars();
    }
  }
}
