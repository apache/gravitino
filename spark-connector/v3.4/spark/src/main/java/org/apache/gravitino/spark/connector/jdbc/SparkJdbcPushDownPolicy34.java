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

import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Set;
import org.apache.gravitino.rel.Column;
import org.apache.spark.sql.connector.expressions.Expression;
import org.apache.spark.sql.connector.expressions.Extract;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.types.StructType;

/** Adds checks for the Extract expression introduced in Spark 3.4. */
class SparkJdbcPushDownPolicy34 extends SparkJdbcPushDownPolicy {
  private static final Set<String> PUSHABLE_EXTRACT_FIELDS =
      ImmutableSet.of("YEAR", "QUARTER", "MONTH", "DAY", "HOUR", "MINUTE");

  SparkJdbcPushDownPolicy34(
      Column[] columns,
      StructType readerSchema,
      StructType sparkSchema,
      JdbcDialect dialect,
      String jdbcUrl) {
    super(columns, readerSchema, sparkSchema, dialect, jdbcUrl);
  }

  @Override
  boolean canPush(Expression expression) {
    if (expression instanceof Extract) {
      Extract extract = (Extract) expression;
      // TIMESTAMP date parts depend on the session time zone. Fractional seconds and week
      // numbering can also differ from Spark at the source.
      if (!PUSHABLE_EXTRACT_FIELDS.contains(extract.field().toUpperCase(Locale.ROOT))
          || referencesZonedTimestamp(extract.source())) {
        return false;
      }
    }
    return super.canPush(expression);
  }
}
