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
package org.apache.gravitino.catalog.hive;

import java.util.List;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestTrinoNativeViewCodec {

  @Test
  void testEncodeDecodeRoundTrip() {
    TrinoNativeViewCodec.ViewDefinition definition =
        new TrinoNativeViewCodec.ViewDefinition(
            "SELECT id, name FROM t",
            "hive",
            "db",
            List.of(
                new TrinoNativeViewCodec.ViewColumn("id", "bigint", null),
                new TrinoNativeViewCodec.ViewColumn("name", "varchar(50)", "name column")),
            "a view comment",
            null,
            true,
            List.of());

    String encoded = TrinoNativeViewCodec.encode(definition);
    Assertions.assertTrue(encoded.startsWith("/* Presto View: "));
    Assertions.assertTrue(encoded.endsWith(" */"));

    TrinoNativeViewCodec.ViewDefinition decoded = TrinoNativeViewCodec.decode(encoded);
    Assertions.assertEquals("SELECT id, name FROM t", decoded.originalSql);
    Assertions.assertEquals("hive", decoded.catalog);
    Assertions.assertEquals("db", decoded.schema);
    Assertions.assertEquals("a view comment", decoded.comment);
    Assertions.assertNull(decoded.owner);
    Assertions.assertTrue(decoded.runAsInvoker);
    Assertions.assertEquals(2, decoded.columns.size());
    Assertions.assertEquals("id", decoded.columns.get(0).name);
    Assertions.assertEquals("bigint", decoded.columns.get(0).type);
    Assertions.assertNull(decoded.columns.get(0).comment);
    Assertions.assertEquals("name", decoded.columns.get(1).name);
    Assertions.assertEquals("varchar(50)", decoded.columns.get(1).type);
    Assertions.assertEquals("name column", decoded.columns.get(1).comment);
  }

  @Test
  void testEncodeDecodeRoundTripWithNullCatalogAndSchema() {
    TrinoNativeViewCodec.ViewDefinition definition =
        new TrinoNativeViewCodec.ViewDefinition(
            "SELECT 1",
            null,
            null,
            List.of(new TrinoNativeViewCodec.ViewColumn("_col0", "integer", null)),
            null,
            null,
            true,
            List.of());

    TrinoNativeViewCodec.ViewDefinition decoded =
        TrinoNativeViewCodec.decode(TrinoNativeViewCodec.encode(definition));
    Assertions.assertNull(decoded.catalog);
    Assertions.assertNull(decoded.schema);
    Assertions.assertNull(decoded.comment);
  }

  @Test
  void testDecodeRejectsMissingPrefixOrSuffix() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TrinoNativeViewCodec.decode("not a presto view"));
  }

  @Test
  void testToTrinoTypeStringForPrimitives() {
    Assertions.assertEquals(
        "boolean", TrinoNativeViewCodec.toTrinoTypeString(Types.BooleanType.get()));
    Assertions.assertEquals(
        "tinyint", TrinoNativeViewCodec.toTrinoTypeString(Types.ByteType.get()));
    Assertions.assertEquals(
        "smallint", TrinoNativeViewCodec.toTrinoTypeString(Types.ShortType.get()));
    Assertions.assertEquals(
        "integer", TrinoNativeViewCodec.toTrinoTypeString(Types.IntegerType.get()));
    Assertions.assertEquals("bigint", TrinoNativeViewCodec.toTrinoTypeString(Types.LongType.get()));
    Assertions.assertEquals("real", TrinoNativeViewCodec.toTrinoTypeString(Types.FloatType.get()));
    Assertions.assertEquals(
        "double", TrinoNativeViewCodec.toTrinoTypeString(Types.DoubleType.get()));
    Assertions.assertEquals(
        "varchar", TrinoNativeViewCodec.toTrinoTypeString(Types.StringType.get()));
    Assertions.assertEquals(
        "varchar(10)", TrinoNativeViewCodec.toTrinoTypeString(Types.VarCharType.of(10)));
    Assertions.assertEquals(
        "char(5)", TrinoNativeViewCodec.toTrinoTypeString(Types.FixedCharType.of(5)));
    Assertions.assertEquals("date", TrinoNativeViewCodec.toTrinoTypeString(Types.DateType.get()));
    Assertions.assertEquals(
        "timestamp(3)",
        TrinoNativeViewCodec.toTrinoTypeString(Types.TimestampType.withoutTimeZone()));
    Assertions.assertEquals(
        "timestamp(3) with time zone",
        TrinoNativeViewCodec.toTrinoTypeString(Types.TimestampType.withTimeZone()));
    Assertions.assertEquals(
        "time(3)", TrinoNativeViewCodec.toTrinoTypeString(Types.TimeType.get()));
    Assertions.assertEquals("uuid", TrinoNativeViewCodec.toTrinoTypeString(Types.UUIDType.get()));
    Assertions.assertEquals(
        "decimal(10,2)", TrinoNativeViewCodec.toTrinoTypeString(Types.DecimalType.of(10, 2)));
    Assertions.assertEquals(
        "varbinary", TrinoNativeViewCodec.toTrinoTypeString(Types.BinaryType.get()));
  }

  @Test
  void testToTrinoTypeStringForComplexTypes() {
    Assertions.assertEquals(
        "array(integer)",
        TrinoNativeViewCodec.toTrinoTypeString(Types.ListType.nullable(Types.IntegerType.get())));
    Assertions.assertEquals(
        "map(varchar,integer)",
        TrinoNativeViewCodec.toTrinoTypeString(
            Types.MapType.valueNullable(Types.StringType.get(), Types.IntegerType.get())));
    Assertions.assertEquals(
        "row(\"a\" integer,\"b\" varchar)",
        TrinoNativeViewCodec.toTrinoTypeString(
            Types.StructType.of(
                Types.StructType.Field.nullableField("a", Types.IntegerType.get()),
                Types.StructType.Field.nullableField("b", Types.StringType.get()))));
  }

  @Test
  void testToTrinoTypeStringPreservesExplicitPrecision() {
    Assertions.assertEquals(
        "timestamp(6)",
        TrinoNativeViewCodec.toTrinoTypeString(Types.TimestampType.withoutTimeZone(6)));
    Assertions.assertEquals(
        "timestamp(6) with time zone",
        TrinoNativeViewCodec.toTrinoTypeString(Types.TimestampType.withTimeZone(6)));
    Assertions.assertEquals(
        "time(6)", TrinoNativeViewCodec.toTrinoTypeString(Types.TimeType.of(6)));
  }

  @Test
  void testToTrinoTypeStringWidensUnsignedIntegralTypes() {
    Assertions.assertEquals(
        "smallint", TrinoNativeViewCodec.toTrinoTypeString(Types.ByteType.unsigned()));
    Assertions.assertEquals(
        "integer", TrinoNativeViewCodec.toTrinoTypeString(Types.ShortType.unsigned()));
    Assertions.assertEquals(
        "bigint", TrinoNativeViewCodec.toTrinoTypeString(Types.IntegerType.unsigned()));
    Assertions.assertEquals(
        "decimal(20,0)", TrinoNativeViewCodec.toTrinoTypeString(Types.LongType.unsigned()));
  }

  @Test
  void testFromTrinoTypeStringForPrimitives() {
    Assertions.assertEquals(
        Types.BooleanType.get(), TrinoNativeViewCodec.fromTrinoTypeString("boolean"));
    Assertions.assertEquals(
        Types.ByteType.get(), TrinoNativeViewCodec.fromTrinoTypeString("tinyint"));
    Assertions.assertEquals(
        Types.ShortType.get(), TrinoNativeViewCodec.fromTrinoTypeString("smallint"));
    Assertions.assertEquals(
        Types.IntegerType.get(), TrinoNativeViewCodec.fromTrinoTypeString("integer"));
    Assertions.assertEquals(
        Types.LongType.get(), TrinoNativeViewCodec.fromTrinoTypeString("bigint"));
    Assertions.assertEquals(
        Types.FloatType.get(), TrinoNativeViewCodec.fromTrinoTypeString("real"));
    Assertions.assertEquals(
        Types.DoubleType.get(), TrinoNativeViewCodec.fromTrinoTypeString("double"));
    Assertions.assertEquals(
        Types.StringType.get(), TrinoNativeViewCodec.fromTrinoTypeString("varchar"));
    Assertions.assertEquals(
        Types.VarCharType.of(10), TrinoNativeViewCodec.fromTrinoTypeString("varchar(10)"));
    Assertions.assertEquals(
        Types.FixedCharType.of(5), TrinoNativeViewCodec.fromTrinoTypeString("char(5)"));
    Assertions.assertEquals(Types.DateType.get(), TrinoNativeViewCodec.fromTrinoTypeString("date"));
    Assertions.assertEquals(
        Types.TimestampType.withoutTimeZone(6),
        TrinoNativeViewCodec.fromTrinoTypeString("timestamp(6)"));
    Assertions.assertEquals(
        Types.TimestampType.withTimeZone(6),
        TrinoNativeViewCodec.fromTrinoTypeString("timestamp(6) with time zone"));
    Assertions.assertEquals(
        Types.TimeType.of(6), TrinoNativeViewCodec.fromTrinoTypeString("time(6)"));
    Assertions.assertEquals(Types.UUIDType.get(), TrinoNativeViewCodec.fromTrinoTypeString("uuid"));
    Assertions.assertEquals(
        Types.DecimalType.of(10, 2), TrinoNativeViewCodec.fromTrinoTypeString("decimal(10,2)"));
    Assertions.assertEquals(
        Types.BinaryType.get(), TrinoNativeViewCodec.fromTrinoTypeString("varbinary"));
  }

  @Test
  void testFromTrinoTypeStringForComplexTypesRoundTripsWithToTrinoTypeString() {
    Types.ListType listType = Types.ListType.nullable(Types.IntegerType.get());
    Assertions.assertEquals(
        listType,
        TrinoNativeViewCodec.fromTrinoTypeString(TrinoNativeViewCodec.toTrinoTypeString(listType)));

    Types.MapType mapType =
        Types.MapType.valueNullable(Types.StringType.get(), Types.IntegerType.get());
    Assertions.assertEquals(
        mapType,
        TrinoNativeViewCodec.fromTrinoTypeString(TrinoNativeViewCodec.toTrinoTypeString(mapType)));

    Types.StructType structType =
        Types.StructType.of(
            Types.StructType.Field.nullableField("a", Types.IntegerType.get()),
            Types.StructType.Field.nullableField("b", Types.StringType.get()));
    Assertions.assertEquals(
        structType,
        TrinoNativeViewCodec.fromTrinoTypeString(
            TrinoNativeViewCodec.toTrinoTypeString(structType)));

    // Nested: array(row(a integer))
    Types.ListType nested =
        Types.ListType.nullable(
            Types.StructType.of(
                Types.StructType.Field.nullableField("a", Types.IntegerType.get())));
    Assertions.assertEquals(
        nested,
        TrinoNativeViewCodec.fromTrinoTypeString(TrinoNativeViewCodec.toTrinoTypeString(nested)));
  }

  @Test
  void testFromTrinoTypeStringHandlesQuotedRowFieldNames() {
    Types.StructType structType =
        Types.StructType.of(
            Types.StructType.Field.nullableField("my field", Types.IntegerType.get()));
    String encoded = TrinoNativeViewCodec.toTrinoTypeString(structType);
    Assertions.assertEquals("row(\"my field\" integer)", encoded);
    Assertions.assertEquals(structType, TrinoNativeViewCodec.fromTrinoTypeString(encoded));
  }

  @Test
  void testToTrinoTypeStringQuotesReservedKeywordRowFieldName() {
    Types.StructType structType =
        Types.StructType.of(
            Types.StructType.Field.nullableField("select", Types.IntegerType.get()));
    String encoded = TrinoNativeViewCodec.toTrinoTypeString(structType);
    Assertions.assertEquals("row(\"select\" integer)", encoded);
    Assertions.assertEquals(structType, TrinoNativeViewCodec.fromTrinoTypeString(encoded));
  }

  @Test
  void testFromTrinoTypeStringRejectsAnonymousRowField() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> TrinoNativeViewCodec.fromTrinoTypeString("row(integer,varchar)"));
  }

  @Test
  void testTimestampTimeAndUuidRoundTripWithToTrinoTypeString() {
    Types.TimestampType timestamp = Types.TimestampType.withoutTimeZone(3);
    Assertions.assertEquals(
        timestamp,
        TrinoNativeViewCodec.fromTrinoTypeString(
            TrinoNativeViewCodec.toTrinoTypeString(timestamp)));

    Types.TimestampType timestampWithTimeZone = Types.TimestampType.withTimeZone(3);
    Assertions.assertEquals(
        timestampWithTimeZone,
        TrinoNativeViewCodec.fromTrinoTypeString(
            TrinoNativeViewCodec.toTrinoTypeString(timestampWithTimeZone)));

    Types.TimeType time = Types.TimeType.of(3);
    Assertions.assertEquals(
        time,
        TrinoNativeViewCodec.fromTrinoTypeString(TrinoNativeViewCodec.toTrinoTypeString(time)));

    Types.UUIDType uuid = Types.UUIDType.get();
    Assertions.assertEquals(
        uuid,
        TrinoNativeViewCodec.fromTrinoTypeString(TrinoNativeViewCodec.toTrinoTypeString(uuid)));
  }

  @Test
  void testFromTrinoTypeStringRejectsUnknownType() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> TrinoNativeViewCodec.fromTrinoTypeString("json"));
  }

  @Test
  void testFromTrinoTypeStringRejectsTimeWithTimeZone() {
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> TrinoNativeViewCodec.fromTrinoTypeString("time(3) with time zone"));
  }

  @Test
  void testDecodeRejectsEmptyColumns() {
    TrinoNativeViewCodec.ViewDefinition definition =
        new TrinoNativeViewCodec.ViewDefinition(
            "SELECT 1", null, null, List.of(), null, null, true, List.of());
    String encoded = TrinoNativeViewCodec.encode(definition);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TrinoNativeViewCodec.decode(encoded));
  }

  @Test
  void testDecodeRejectsMalformedBase64Payload() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TrinoNativeViewCodec.decode("/* Presto View: not-valid-base64!! */"));
  }
}
