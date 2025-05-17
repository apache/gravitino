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
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.TimestampType;

public class SparkHiveTypeConverter extends SparkTypeConverter {

  /**
   * Converts Spark data types to Hive-compatible Gravitino types
   *
   * <p>Special handling for timestamp types:
   *
   * <ul>
   *   <li>{@link TimestampType} ➔ {@link Types.TimestampType#withoutTimeZone()} (Hive limitation)
   *   <li>Other types follow base class conversion rules
   * </ul>
   *
   * @param sparkType Spark data type to convert (non-null)
   * @return Hive-compatible Gravitino type with timezone information removed where applicable
   * @throws UnsupportedOperationException For unsupported timestamp conversions
   * @see <a href="https://github.com/apache/gravitino/issues/7046">Issue #7046</a>
   */
  @Override
  public Type toGravitinoType(DataType sparkType) {
    if (sparkType instanceof TimestampType) {
      return Types.TimestampType.withoutTimeZone();
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
