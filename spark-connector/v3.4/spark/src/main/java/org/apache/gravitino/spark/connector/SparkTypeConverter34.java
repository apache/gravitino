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
package org.apache.gravitino.spark.connector;

import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.TimestampNTZType;

public class SparkTypeConverter34 extends SparkTypeConverter {

  @Override
  public Type toGravitinoType(DataType sparkType) {
    if (sparkType instanceof TimestampNTZType) {
      return Types.TimestampType.withoutTimeZone();
    } else {
      return super.toGravitinoType(sparkType);
    }
  }

  @Override
  public DataType toSparkType(Type gravitinoType) {
    if (gravitinoType instanceof Types.TimestampType
        && !((Types.TimestampType) gravitinoType).hasTimeZone()) {
      return DataTypes.TimestampNTZType;
    } else {
      return super.toSparkType(gravitinoType);
    }
  }
}
