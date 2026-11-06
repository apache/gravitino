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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Locale;
import org.apache.gravitino.catalog.jdbc.converter.JdbcColumnDefaultValueConverter;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.dto.requests.TableUpdateRequest.UpdateTableColumnDefaultValueRequest;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.types.Decimal;
import org.apache.gravitino.rel.types.Types;
import org.apache.spark.sql.catalyst.expressions.Cast;
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser$;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSparkTableChangeConverter34 {
  private SparkTableChangeConverter sparkTableChangeConverter =
      new SparkTableChangeConverter34(new SparkTypeConverter34());

  @Test
  void testTransformSetProperty() {
    TableChange sparkSetProperty = TableChange.setProperty("key", "value");
    org.apache.gravitino.rel.TableChange tableChange =
        sparkTableChangeConverter.toGravitinoTableChange(sparkSetProperty);
    Assertions.assertTrue(tableChange instanceof org.apache.gravitino.rel.TableChange.SetProperty);
    org.apache.gravitino.rel.TableChange.SetProperty gravitinoSetProperty =
        (org.apache.gravitino.rel.TableChange.SetProperty) tableChange;
    Assertions.assertEquals("key", gravitinoSetProperty.getProperty());
    Assertions.assertEquals("value", gravitinoSetProperty.getValue());
  }

  @Test
  void testTransformUpdateColumnDefaultValue() {
    String[] fieldNames = new String[] {"col"};
    String defaultValue = "'col_default_value'";
    TableChange.UpdateColumnDefaultValue sparkChange =
        (TableChange.UpdateColumnDefaultValue)
            TableChange.updateColumnDefaultValue(fieldNames, defaultValue);

    org.apache.gravitino.rel.TableChange gravitinoChange =
        sparkTableChangeConverter.toGravitinoTableChange(
            sparkChange, () -> new Column[] {Column.of("col", Types.StringType.get(), null)});

    Assertions.assertInstanceOf(
        org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue.class, gravitinoChange);
    org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue converted =
        (org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue) gravitinoChange;
    Assertions.assertArrayEquals(sparkChange.fieldNames(), converted.fieldName());
    Assertions.assertEquals(
        Literals.stringLiteral("col_default_value"), converted.getNewDefaultValue());
  }

  @Test
  void testDefaultSqlIsConvertedToTypedLiterals() {
    assertDefault("42", Literals.integerLiteral(42));
    assertDefault("-42", Literals.integerLiteral(-42));
    assertDefault("12.34", Literals.decimalLiteral(Decimal.of("12.34", 4, 2)));
    assertDefault("true", Literals.booleanLiteral(true));
    assertDefault("NULL", Literals.NULL);
    assertDefault("''", Literals.stringLiteral(""));
    assertDefault("DATE '2026-11-04'", Literals.dateLiteral("2026-11-04"));
    UnsupportedOperationException exception =
        Assertions.assertThrows(
            UnsupportedOperationException.class,
            () ->
                sparkTableChangeConverter.toGravitinoTableChange(
                    TableChange.updateColumnDefaultValue(new String[] {"col"}, "")));
    Assertions.assertEquals("DROP DEFAULT is not supported by Gravitino", exception.getMessage());
    for (String expression :
        new String[] {
          "1 + 1",
          "current_timestamp()",
          "upper('abc')",
          "not valid SQL!",
          "column_name",
          "+(1 + 1)",
          "+'abc'",
          "+true"
        }) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              sparkTableChangeConverter.toGravitinoTableChange(
                  TableChange.updateColumnDefaultValue(new String[] {"col"}, expression)));
    }
  }

  @Test
  void testExplicitPositiveLiteralDefaults() {
    assertDefault("+42", Literals.integerLiteral(42));
    assertDefault("+42L", Literals.longLiteral(42L));
    assertDefault("+12.34", Literals.decimalLiteral(Decimal.of("12.34", 4, 2)));
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
  void testTimestampDefaultsPreserveInstant(String timeZone) {
    withSessionConfig(
        timeZone,
        "TIMESTAMP_LTZ",
        () -> {
          for (String text :
              new String[] {
                "2026-11-04T12:34:56.123456",
                "1969-12-31T23:59:59.999999",
                "2026-11-04T12:34:00",
                "2026-11-04T00:00:00"
              }) {
            Expression expected =
                Literals.of(
                    LocalDateTime.parse(text)
                        .atZone(ZoneId.of(timeZone))
                        .toInstant()
                        .atOffset(ZoneOffset.UTC),
                    Types.TimestampType.withTimeZone());
            Expression converted = assertDefault("TIMESTAMP '" + text + "'", expected);
            String expectedSql =
                "'"
                    + ((Literal<?>) expected)
                        .value()
                        .toString()
                        .replace("T", " ")
                        .replace("Z", "+00:00")
                        .replaceFirst("^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2})(?=[+-])", "$1:00")
                    + "'";
            Assertions.assertEquals(
                expectedSql,
                new JdbcColumnDefaultValueConverter()
                    .fromGravitino(DTOConverters.toFunctionArg(converted)));
            assertDefault("TIMESTAMP_LTZ '" + text + "'", expected);
          }
          assertDefault(
              "TIMESTAMP '2026-11-04 12:34:56.123456+02:00'",
              Literals.of(
                  LocalDateTime.parse("2026-11-04T10:34:56.123456").atOffset(ZoneOffset.UTC),
                  Types.TimestampType.withTimeZone()));
        });
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
  void testTimestampNtzDefaultsPreserveLocalTime(String timeZone) {
    withSessionConfig(
        timeZone,
        "TIMESTAMP_NTZ",
        () -> {
          for (String text :
              new String[] {
                "2026-11-04T12:34:56.123456",
                "1969-12-31T23:59:59.999999",
                "2026-11-04T12:34:00",
                "2026-11-04T00:00:00"
              }) {
            Expression expected = Literals.timestampLiteral(text);
            assertDefault("TIMESTAMP_NTZ '" + text + "'", expected);
            assertDefault("TIMESTAMP '" + text + "'", expected);
          }
        });
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles"})
  void testTimestampDefaultsAreCoercedToTargetColumn(String timeZone) {
    for (String timestampType : new String[] {"TIMESTAMP_LTZ", "TIMESTAMP_NTZ"}) {
      withSessionConfig(
          timeZone,
          timestampType,
          () -> {
            String text = "2026-11-04T12:34:56.123456";
            assertDefault("TIMESTAMP_LTZ '" + text + "'", Literals.timestampLiteral(text));
            try {
              Cast cast =
                  (Cast)
                      CatalystSqlParser$.MODULE$.parseExpression(
                          "CAST(TIMESTAMP_LTZ '" + text + "' AS TIMESTAMP_NTZ)");
              Assertions.assertEquals(
                  LocalDateTime.parse(text),
                  DateTimeUtils.microsToLocalDateTime(
                      (long) ((Cast) cast.withTimeZone(timeZone)).eval(null)));
            } catch (Exception e) {
              throw new AssertionError(e);
            }
            assertDefault(
                "TIMESTAMP_NTZ '" + text + "'",
                Literals.of(
                    LocalDateTime.parse(text)
                        .atZone(ZoneId.of(timeZone))
                        .toInstant()
                        .atOffset(ZoneOffset.UTC),
                    Types.TimestampType.withTimeZone()));
            assertDefault(
                "TIMESTAMP_LTZ '2026-11-04 12:34:56.123456+02:00'",
                Literals.timestampLiteral(
                    LocalDateTime.parse(text)
                        .atOffset(ZoneOffset.ofHours(2))
                        .toInstant()
                        .atZone(ZoneId.of(timeZone))
                        .toLocalDateTime()));
          });
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Asia/Shanghai", "America/Los_Angeles", "Pacific/Apia"})
  void testDateDefaultsAreCoercedToTimestampTarget(String timeZone) {
    for (String timestampType : new String[] {"TIMESTAMP_LTZ", "TIMESTAMP_NTZ"}) {
      withSessionConfig(
          timeZone,
          timestampType,
          () -> {
            for (String date : new String[] {"2026-11-04", "1969-12-31", "2011-12-30"}) {
              String sql = "DATE '" + date + "'";
              for (String target : new String[] {"TIMESTAMP_LTZ", "TIMESTAMP_NTZ"}) {
                try {
                  Cast cast =
                      (Cast)
                          CatalystSqlParser$.MODULE$.parseExpression(
                              "CAST(" + sql + " AS " + target + ")");
                  long micros = (long) ((Cast) cast.withTimeZone(timeZone)).eval(null);
                  Expression expected =
                      target.equals("TIMESTAMP_LTZ")
                          ? Literals.of(
                              DateTimeUtils.microsToInstant(micros).atOffset(ZoneOffset.UTC),
                              Types.TimestampType.withTimeZone())
                          : Literals.timestampLiteral(DateTimeUtils.microsToLocalDateTime(micros));
                  Expression converted = assertDefault(sql, expected);
                  UpdateTableColumnDefaultValueRequest request =
                      new UpdateTableColumnDefaultValueRequest(
                          new String[] {"col"}, DTOConverters.toFunctionArg(converted));
                  UpdateTableColumnDefaultValueRequest restored =
                      JsonUtils.objectMapper()
                          .readValue(
                              JsonUtils.objectMapper().writeValueAsString(request),
                              UpdateTableColumnDefaultValueRequest.class);
                  String expectedSql =
                      target.equals("TIMESTAMP_LTZ")
                          ? "'"
                              + LocalDate.parse(date)
                                  .atStartOfDay(ZoneId.of(timeZone))
                                  .toInstant()
                                  .atOffset(ZoneOffset.UTC)
                                  .toString()
                                  .replace("T", " ")
                                  .replace("Z", "+00:00")
                                  .replaceFirst(
                                      "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2})(?=[+-])", "$1:00")
                              + "'"
                          : "'" + date + " 00:00:00'";
                  Assertions.assertEquals(
                      expectedSql,
                      new JdbcColumnDefaultValueConverter()
                          .fromGravitino(restored.getNewDefaultValue()));
                } catch (Exception e) {
                  throw new AssertionError(e);
                }
              }
            }
          });
    }
  }

  @Test
  void testStringDefaultsRejectTimestampTargets() {
    for (Types.TimestampType target :
        new Types.TimestampType[] {
          Types.TimestampType.withTimeZone(), Types.TimestampType.withoutTimeZone()
        }) {
      for (String sql :
          new String[] {"'2026-11-04'", "'2026-11-04 12:34:56.123456'", "'now'", "''"}) {
        IllegalArgumentException error =
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                    sparkTableChangeConverter.toGravitinoTableChange(
                        TableChange.updateColumnDefaultValue(new String[] {"col"}, sql),
                        () -> new Column[] {Column.of("col", target, null)}));
        Assertions.assertTrue(error.getMessage().contains("col"));
        Assertions.assertTrue(error.getMessage().contains("TIMESTAMP_LTZ or TIMESTAMP_NTZ"));
      }
    }
    assertDefault(
        "'2026-11-04 12:34:56.123456'", Literals.stringLiteral("2026-11-04 12:34:56.123456"));
    assertDefault("''", Literals.stringLiteral(""));
    assertDefault("DATE '2026-11-04'", Literals.dateLiteral("2026-11-04"));
  }

  @Test
  void testNestedTimestampTargetAndInvalidColumnPaths() {
    Column[] columns =
        new Column[] {
          Column.of(
              "parent",
              Types.StructType.of(
                  Types.StructType.Field.nullableField(
                      "ts", Types.TimestampType.withoutTimeZone())),
              null)
        };
    org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue converted =
        (org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue)
            sparkTableChangeConverter.toGravitinoTableChange(
                TableChange.updateColumnDefaultValue(
                    new String[] {"parent", "ts"}, "TIMESTAMP_NTZ '2026-11-04 12:34:00'"),
                () -> columns);
    Assertions.assertEquals(
        Literals.timestampLiteral("2026-11-04T12:34:00"), converted.getNewDefaultValue());
    for (String[] path :
        new String[][] {{"missing"}, {"parent", "missing"}, {"parent", "ts", "child"}}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              sparkTableChangeConverter.toGravitinoTableChange(
                  TableChange.updateColumnDefaultValue(path, "TIMESTAMP_NTZ '2026-11-04 12:34:00'"),
                  () -> columns));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"TIMESTAMP '2026-11-04 12:34:56'", "DATE '2026-11-04'", "'2026-11-04'"})
  void testTemporalDefaultRequiresTargetSchema(String sql) {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            sparkTableChangeConverter.toGravitinoTableChange(
                TableChange.updateColumnDefaultValue(new String[] {"col"}, sql)));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> assertDefault("TIMESTAMP '2026-11-04 12:34:56'", Literals.stringLiteral("unused")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"4142", "0080FF", ""})
  void testBinaryLiteralDefaults(String hex) throws Exception {
    Expression converted =
        assertDefault(
            "X'" + hex + "'", Literals.of(HexFormat.of().parseHex(hex), Types.BinaryType.get()));
    LiteralDTO dto = (LiteralDTO) DTOConverters.toFunctionArg(converted);
    Assertions.assertEquals("0x" + hex.toLowerCase(Locale.ROOT), dto.value());
    UpdateTableColumnDefaultValueRequest request =
        new UpdateTableColumnDefaultValueRequest(new String[] {"col"}, dto);
    UpdateTableColumnDefaultValueRequest restored =
        JsonUtils.objectMapper()
            .readValue(
                JsonUtils.objectMapper().writeValueAsString(request),
                UpdateTableColumnDefaultValueRequest.class);
    Assertions.assertEquals(
        "X'" + hex.toLowerCase(Locale.ROOT) + "'",
        new JdbcColumnDefaultValueConverter().fromGravitino(restored.getNewDefaultValue()));
  }

  private Expression assertDefault(String sql, Expression expected) {
    org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue converted =
        (org.apache.gravitino.rel.TableChange.UpdateColumnDefaultValue)
            sparkTableChangeConverter.toGravitinoTableChange(
                TableChange.updateColumnDefaultValue(new String[] {"col"}, sql),
                () -> new Column[] {Column.of("col", ((Literal<?>) expected).dataType(), null)});
    Literal<?> expectedLiteral = (Literal<?>) expected;
    if (expectedLiteral.value() instanceof byte[]) {
      Literal<?> actual = (Literal<?>) converted.getNewDefaultValue();
      Assertions.assertEquals(expectedLiteral.dataType(), actual.dataType());
      Assertions.assertArrayEquals((byte[]) expectedLiteral.value(), (byte[]) actual.value());
    } else {
      Assertions.assertEquals(expected, converted.getNewDefaultValue());
    }
    return converted.getNewDefaultValue();
  }

  private void withSessionConfig(String timeZone, String timestampType, Runnable assertions) {
    SQLConf conf = new SQLConf();
    conf.setConfString("spark.sql.session.timeZone", timeZone);
    conf.setConfString("spark.sql.timestampType", timestampType);
    SQLConf.withExistingConf(
        conf,
        () -> {
          assertions.run();
          return null;
        });
  }
}
