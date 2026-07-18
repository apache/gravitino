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

package org.apache.gravitino.trino.connector.catalog.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarcharType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.GravitinoMetadata;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorMetadata;
import org.apache.gravitino.trino.connector.metadata.GravitinoColumn;
import org.apache.gravitino.trino.connector.metadata.GravitinoTable;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

public class TestIcebergDataTypeTransformer {

  private static final int TRINO_478 = 478;

  @Test
  public void testTrinoTypeToGravitinoType() {
    GeneralDataTypeTransformer generalDataTypeTransformer =
        new IcebergDataTypeTransformer(TRINO_478);
    io.trino.spi.type.Type charTypeWithLengthOne = io.trino.spi.type.CharType.createCharType(1);

    Exception e =
        Assertions.assertThrows(
            TrinoException.class,
            () -> generalDataTypeTransformer.getGravitinoType(charTypeWithLengthOne));
    Assertions.assertTrue(e.getMessage().contains("Iceberg does not support the datatype CHAR"));

    io.trino.spi.type.Type varcharType = io.trino.spi.type.VarcharType.createVarcharType(1);
    e =
        Assertions.assertThrows(
            TrinoException.class, () -> generalDataTypeTransformer.getGravitinoType(varcharType));
    Assertions.assertTrue(
        e.getMessage().contains("Iceberg does not support the datatype VARCHAR with length"));

    io.trino.spi.type.Type varcharTypeWithoutLength = VarcharType.VARCHAR;

    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(varcharTypeWithoutLength),
        Types.StringType.get());

    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(Types.TimeType.get()), TimeType.TIME_MICROS);

    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(Types.TimestampType.withoutTimeZone()),
        TimestampType.TIMESTAMP_MICROS);

    Assertions.assertEquals(
        generalDataTypeTransformer.getTrinoType(Types.TimestampType.withTimeZone()),
        TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS);

    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(TimestampType.TIMESTAMP_MILLIS),
        Types.TimestampType.withoutTimeZone(6));
    Assertions.assertEquals(
        generalDataTypeTransformer.getGravitinoType(TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS),
        Types.TimestampType.withTimeZone(6));
  }

  @Test
  public void testRejectNanosecondTimestamps() {
    IcebergDataTypeTransformer transformer = new IcebergDataTypeTransformer(TRINO_478);
    Type[] gravitinoTypes = {
      Types.TimestampType.withoutTimeZone(9), Types.TimestampType.withTimeZone(9)
    };
    io.trino.spi.type.Type[] trinoTypes = {
      TimestampType.createTimestampType(9),
      TimestampWithTimeZoneType.createTimestampWithTimeZoneType(9)
    };

    for (Type type : gravitinoTypes) {
      assertIllegalArgument(() -> transformer.getTrinoType(type), "requires Trino 481 or newer");
      assertMetadataRejectedBeforeConnectorInvocation(type);
    }
    for (io.trino.spi.type.Type type : trinoTypes) {
      assertIllegalArgument(
          () -> transformer.getGravitinoType(type), "requires Trino 481 or newer");
    }
  }

  @Test
  public void testNanosecondTimestampsStartInTrino481() {
    IcebergDataTypeTransformer transformer = new IcebergDataTypeTransformer(481);

    Assertions.assertEquals(
        transformer.getTrinoType(Types.TimestampType.withoutTimeZone(9)),
        TimestampType.createTimestampType(9));
    Assertions.assertEquals(
        transformer.getTrinoType(Types.TimestampType.withTimeZone(9)),
        TimestampWithTimeZoneType.createTimestampWithTimeZoneType(9));
    Assertions.assertEquals(
        transformer.getGravitinoType(TimestampType.createTimestampType(9)),
        Types.TimestampType.withoutTimeZone(9));
    Assertions.assertEquals(
        transformer.getGravitinoType(TimestampWithTimeZoneType.createTimestampWithTimeZoneType(9)),
        Types.TimestampType.withTimeZone(9));
  }

  @Test
  public void testRejectTimestampPrecisionWithoutIcebergRepresentation() {
    IcebergDataTypeTransformer transformer = new IcebergDataTypeTransformer(482);

    assertIllegalArgument(
        () -> transformer.getTrinoType(Types.TimestampType.withoutTimeZone(7)),
        "timestamp precision must be 6 or 9");
    assertIllegalArgument(
        () -> transformer.getGravitinoType(TimestampType.createTimestampType(12)),
        "timestamp precision must be 6 or 9");
  }

  @Test
  public void testCreateTableRejectsBeforeCatalogMutation() {
    CatalogConnectorMetadata catalogMetadata = Mockito.mock(CatalogConnectorMetadata.class);
    ConnectorMetadata internalMetadata = Mockito.mock(ConnectorMetadata.class);
    IcebergMetadataAdapter adapter =
        (IcebergMetadataAdapter) new IcebergConnectorAdapter().getMetadataAdapter(TRINO_478);
    GravitinoMetadata metadata =
        new GravitinoMetadata(catalogMetadata, adapter, internalMetadata) {};
    ConnectorTableMetadata tableMetadata =
        new ConnectorTableMetadata(
            new SchemaTableName("schema", "unsupported_type"),
            ImmutableList.of(
                ColumnMetadata.builder()
                    .setName("col")
                    .setType(TimestampType.createTimestampType(9))
                    .build()),
            ImmutableMap.of(IcebergPropertyMeta.ICEBERG_FORMAT_PROPERTY, "PARQUET"));

    assertIllegalArgument(
        () -> metadata.createTable(null, tableMetadata, SaveMode.FAIL),
        "current Trino version is 478");
    Mockito.verifyNoInteractions(catalogMetadata, internalMetadata);
  }

  @Test
  public void testRejectVariant() {
    IcebergDataTypeTransformer transformer = new IcebergDataTypeTransformer(TRINO_478);
    String expectedMessage = "requires Trino 481 or newer";

    assertIllegalArgument(() -> transformer.getTrinoType(Types.VariantType.get()), expectedMessage);
    assertMetadataRejectedBeforeConnectorInvocation(Types.VariantType.get(), expectedMessage);
    assertIllegalArgument(
        () -> transformer.getGravitinoType(mockTrinoType("variant")), expectedMessage);
  }

  @Test
  public void testRejectUnknown() {
    IcebergDataTypeTransformer transformer = new IcebergDataTypeTransformer(TRINO_478);
    String expectedMessage = "not supported by any Trino Iceberg connector version";

    assertIllegalArgument(() -> transformer.getTrinoType(Types.NullType.get()), expectedMessage);
    assertMetadataRejectedBeforeConnectorInvocation(Types.NullType.get(), expectedMessage);
    assertIllegalArgument(
        () -> transformer.getGravitinoType(mockTrinoType("unknown")), expectedMessage);
  }

  @Test
  public void testRejectGeometry() {
    IcebergDataTypeTransformer transformer = new IcebergDataTypeTransformer(TRINO_478);
    String expectedMessage = "requires Trino 482 or newer";
    Type type = Types.GeometryType.of("EPSG:3857");

    assertIllegalArgument(() -> transformer.getTrinoType(type), expectedMessage);
    assertMetadataRejectedBeforeConnectorInvocation(type, expectedMessage);
    assertIllegalArgument(
        () -> transformer.getGravitinoType(mockTrinoType("geometry")), expectedMessage);
  }

  @Test
  public void testRejectGeography() {
    IcebergDataTypeTransformer transformer = new IcebergDataTypeTransformer(TRINO_478);
    String expectedMessage = "requires Trino 482 or newer";
    Type type = Types.GeographyType.of("EPSG:4326", "karney");

    assertIllegalArgument(() -> transformer.getTrinoType(type), expectedMessage);
    assertMetadataRejectedBeforeConnectorInvocation(type, expectedMessage);
    assertIllegalArgument(
        () -> transformer.getGravitinoType(mockTrinoType("geography")), expectedMessage);
    assertIllegalArgument(
        () -> transformer.getGravitinoType(mockTrinoType("sphericalgeography")), expectedMessage);
  }

  @Test
  public void testRejectTypesWithoutLosslessGravitinoConversionOnNewerTrino() {
    IcebergDataTypeTransformer trino481Transformer = new IcebergDataTypeTransformer(481);
    assertIllegalArgument(
        () -> trino481Transformer.getTrinoType(Types.VariantType.get()),
        "cannot losslessly convert Iceberg V3 variant on Trino 481");
    assertIllegalArgument(
        () -> trino481Transformer.getGravitinoType(mockTrinoType("variant")),
        "cannot losslessly convert Iceberg V3 variant on Trino 481");

    IcebergDataTypeTransformer trino482Transformer = new IcebergDataTypeTransformer(482);
    assertIllegalArgument(
        () -> trino482Transformer.getTrinoType(Types.GeometryType.of("EPSG:3857")),
        "cannot losslessly convert Iceberg V3 geometry on Trino 482");
    assertIllegalArgument(
        () -> trino482Transformer.getGravitinoType(mockTrinoType("geometry")),
        "cannot losslessly convert Iceberg V3 geometry on Trino 482");
    assertIllegalArgument(
        () -> trino482Transformer.getTrinoType(Types.GeographyType.of("EPSG:4326", "spherical")),
        "cannot losslessly convert Iceberg V3 geography on Trino 482");
    assertIllegalArgument(
        () -> trino482Transformer.getGravitinoType(mockTrinoType("geography")),
        "cannot losslessly convert Iceberg V3 geography on Trino 482");
  }

  private static void assertMetadataRejectedBeforeConnectorInvocation(Type type) {
    assertMetadataRejectedBeforeConnectorInvocation(type, "requires Trino 481 or newer");
  }

  private static void assertMetadataRejectedBeforeConnectorInvocation(
      Type type, String expectedMessage) {
    IcebergMetadataAdapter adapter =
        new IcebergMetadataAdapter(
            ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), TRINO_478);
    GravitinoTable table =
        new GravitinoTable(
            "schema",
            "unsupported_type",
            ImmutableList.of(new GravitinoColumn("col", type, 0, "", true)),
            "",
            ImmutableMap.of());

    assertIllegalArgument(() -> adapter.getTableMetadata(table), expectedMessage);
  }

  private static io.trino.spi.type.Type mockTrinoType(String baseType) {
    io.trino.spi.type.Type type = Mockito.mock(io.trino.spi.type.Type.class);
    Mockito.when(type.getTypeSignature()).thenReturn(new TypeSignature(baseType));
    return type;
  }

  private static void assertIllegalArgument(Executable executable, String expectedMessage) {
    TrinoException exception = Assertions.assertThrows(TrinoException.class, executable);
    Assertions.assertEquals(
        GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT.toErrorCode(), exception.getErrorCode());
    Assertions.assertTrue(exception.getMessage().contains(expectedMessage));
  }
}
