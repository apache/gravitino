/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.catalog.starrocks.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestStarRocksTypeConverter {

  private static final StarRocksTypeConverter TYPE_CONVERTER = new StarRocksTypeConverter();

  @Test
  void testTimestampMapping() {
    Assertions.assertEquals(Types.TimestampType.withoutTimeZone(), toGravitino("datetime", null));
    Assertions.assertEquals(Types.TimestampType.withoutTimeZone(), toGravitino("datetime", 0));
    Assertions.assertEquals(Types.TimestampType.withoutTimeZone(), toGravitino("datetime", 6));

    Assertions.assertEquals(
        "datetime", TYPE_CONVERTER.fromGravitino(Types.TimestampType.withoutTimeZone()));
  }

  @Test
  void testRejectPrecisionQualifiedTimestampTypes() {
    Type[] timestampTypes = {
      Types.TimestampType.withoutTimeZone(3), Types.TimestampType.withoutTimeZone(9)
    };
    for (Type timestampType : timestampTypes) {
      IllegalArgumentException timestampException =
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> TYPE_CONVERTER.fromGravitino(timestampType));
      Assertions.assertTrue(
          timestampException
              .getMessage()
              .contains(
                  "StarRocks DATETIME columns do not preserve declared fractional precision"));
    }

    IllegalArgumentException timestampTzException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> TYPE_CONVERTER.fromGravitino(Types.TimestampType.withTimeZone(9)));
    Assertions.assertTrue(
        timestampTzException
            .getMessage()
            .contains("StarRocks DATETIME does not preserve time-zone semantics"));
  }

  @Test
  void testRejectVariantType() {
    Assertions.assertEquals(Types.ExternalType.of("json"), toGravitino("json", null));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> TYPE_CONVERTER.fromGravitino(Types.VariantType.get()));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("StarRocks JSON is not an exact representation of Gravitino Variant"));
  }

  private static Type toGravitino(String typeName, Integer datetimePrecision) {
    JdbcTypeConverter.JdbcTypeBean typeBean = new JdbcTypeConverter.JdbcTypeBean(typeName);
    typeBean.setDatetimePrecision(datetimePrecision);
    return TYPE_CONVERTER.toGravitino(typeBean);
  }
}
