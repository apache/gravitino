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

package org.apache.gravitino.spark.connector.hive;

import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.SparkTypeConverter34;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

public class SparkHiveTypeConverter34 extends SparkTypeConverter34 {
  /**
   * Converts Spark timestamp types to Hive-compatible Gravitino types
   *
   * <p>This implementation enforces consistent timestamp handling across Spark versions:
   *
   * <ol>
   *   <li>Converts Spark's {@link TimestampType} (with timezone) to {@link
   *       Types.TimestampType#withoutTimeZone()}
   *   <li>Explicitly rejects {@link TimestampNTZType} as it's incompatible with Hive's type system
   * </ol>
   *
   * @param sparkType Spark data type to convert, must not be null
   * @return Gravitino type with timezone information stripped when necessary
   * @throws UnsupportedOperationException When encountering {@link TimestampNTZType}
   * @see <a href="https://github.com/apache/gravitino/issues/7046">Issue #7046</a>
   * @see SparkTypeConverter34#toGravitinoType(DataType) Base implementation
   */
  @Override
  public Type toGravitinoType(DataType sparkType) {
    if (sparkType instanceof TimestampType) {
      return Types.TimestampType.withoutTimeZone();
    } else if (sparkType instanceof TimestampNTZType) {
      throw new UnsupportedOperationException(
          "Hive does not support 'timestamp_ntz' (timestamp without time zone), please use 'timestamp' instead.");
    } else {
      return super.toGravitinoType(sparkType);
    }
  }

  /**
   * Converts Gravitino types to Spark data types.
   *
   * <p>Note: All timestamp types convert to Spark's TimestampType.
   */
  @Override
  public DataType toSparkType(Type gravitinoType) {
    if (gravitinoType instanceof Types.TimestampType) {
      return DataTypes.TimestampType;
    } else {
      return super.toSparkType(gravitinoType);
    }
  }
}
