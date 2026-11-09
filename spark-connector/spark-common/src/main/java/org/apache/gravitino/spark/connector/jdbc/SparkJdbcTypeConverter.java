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

package org.apache.gravitino.spark.connector.jdbc;

import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.spark.connector.SparkTypeConverter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkJdbcTypeConverter extends SparkTypeConverter {

  private static final Logger LOG = LoggerFactory.getLogger(SparkJdbcTypeConverter.class);

  private final boolean useTimestampNtz;

  /** Creates a converter retaining the existing JDBC timestamp mapping. */
  public SparkJdbcTypeConverter() {
    this(false);
  }

  /**
   * Creates a converter with provider-specific timestamp handling.
   *
   * <p>When {@code useTimestampNtz} is true, the Spark JDBC reader of the same catalog must also
   * use {@code preferTimestampNTZ=true}. Otherwise the table schema and the scanned rows use
   * different timestamp types.
   *
   * @param useTimestampNtz whether zone-free timestamps use Spark TimestampNTZType
   */
  public SparkJdbcTypeConverter(boolean useTimestampNtz) {
    this.useTimestampNtz = useTimestampNtz;
  }

  @Override
  public DataType toSparkType(Type gravitinoType) {
    // if spark version lower than 3.4.4, using VarCharType will throw an exception: Unsupported
    // type varchar.
    if (gravitinoType instanceof Types.VarCharType) {
      return DataTypes.StringType;
    } else if (gravitinoType instanceof Types.ExternalType) {
      // An external type carries a source type that Gravitino cannot represent, such as an
      // unconstrained PostgreSQL numeric. Reading it as a string keeps an unmapped type from
      // failing the type conversion of the whole table.
      LOG.warn(
          "Reading type {} as a string, Gravitino cannot represent it",
          ((Types.ExternalType) gravitinoType).catalogString());
      return DataTypes.StringType;
<<<<<<< HEAD
    } else if (gravitinoType instanceof Types.TimestampType) {
=======
    } else if (gravitinoType instanceof Types.TimestampType && !useTimestampNtz) {
      // Both timestamp flavors map to TimestampType, never TimestampNTZType: the MySQL dialect only
      // pushes TimestampType literals down correctly, and NTZ literals produce invalid SQL.
>>>>>>> 7f88982b8 ([#14642] fix(jdbc): correct Spark reads and Trino metadata handling (#14643))
      return DataTypes.TimestampType;
    } else {
      return super.toSparkType(gravitinoType);
    }
  }
}
