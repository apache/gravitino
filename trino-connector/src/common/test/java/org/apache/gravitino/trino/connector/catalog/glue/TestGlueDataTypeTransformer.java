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
package org.apache.gravitino.trino.connector.catalog.glue;

import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestGlueDataTypeTransformer {

  private final GlueDataTypeTransformer transformer = new GlueDataTypeTransformer();

  @Test
  void testTrinoTimeToGravitinoType() {
    // Any Trino TIME precision should map to Gravitino TIME(6) for Iceberg compatibility.
    Assertions.assertEquals(
        Types.TimeType.of(6), transformer.getGravitinoType(TimeType.TIME_MILLIS));
    Assertions.assertEquals(
        Types.TimeType.of(6), transformer.getGravitinoType(TimeType.TIME_MICROS));
    Assertions.assertEquals(
        Types.TimeType.of(6), transformer.getGravitinoType(TimeType.TIME_NANOS));
  }

  @Test
  void testTrinoTimestampWithTimeZoneToGravitinoType() {
    // TIMESTAMP WITH TIME ZONE should map to Gravitino TIMESTAMP WITH TIME ZONE(6).
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(6),
        transformer.getGravitinoType(TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS));
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(6),
        transformer.getGravitinoType(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS));
  }

  @Test
  void testGravitinoTimeToTrinoType() {
    // Gravitino TIME should always map to Trino TIME_MICROS.
    Assertions.assertEquals(TimeType.TIME_MICROS, transformer.getTrinoType(Types.TimeType.of(3)));
    Assertions.assertEquals(TimeType.TIME_MICROS, transformer.getTrinoType(Types.TimeType.of(6)));
  }

  @Test
  void testGravitinoTimestampWithTimeZoneToTrinoType() {
    // Gravitino TIMESTAMP WITH TIME ZONE should map to Trino TIMESTAMP_TZ_MICROS.
    Assertions.assertEquals(
        TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS,
        transformer.getTrinoType(Types.TimestampType.withTimeZone(6)));
  }

  @Test
  void testGravitinoTimestampWithPrecisionToTrinoType() {
    // Gravitino TIMESTAMP with explicit precision (Iceberg-originated) maps to TIMESTAMP_MICROS.
    Assertions.assertEquals(
        TimestampType.TIMESTAMP_MICROS,
        transformer.getTrinoType(Types.TimestampType.withoutTimeZone(6)));
  }

  @Test
  void testGravitinoTimestampNoPrecisionToTrinoType() {
    // Gravitino TIMESTAMP without precision (Hive/Glue-originated) falls through to
    // TIMESTAMP_MILLIS.
    Assertions.assertEquals(
        TimestampType.TIMESTAMP_MILLIS,
        transformer.getTrinoType(Types.TimestampType.withoutTimeZone()));
  }
}
