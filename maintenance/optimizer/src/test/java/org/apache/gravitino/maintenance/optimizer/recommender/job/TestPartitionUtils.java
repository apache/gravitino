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
package org.apache.gravitino.maintenance.optimizer.recommender.job;

import java.util.Arrays;
import java.util.List;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionEntry;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionUtils {

  private static Column column(String name, org.apache.gravitino.rel.types.Type type) {
    return ColumnDTO.builder().withName(name).withDataType(type).build();
  }

  private static Column eventStructColumn() {
    return column(
        "event",
        Types.StructType.of(
            Types.StructType.Field.notNullField("ts", Types.TimestampType.withoutTimeZone())));
  }

  @Test
  void testIdentityStringPartition() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("colA", "O'Hara"));
    Column[] columns = new Column[] {column("colA", Types.StringType.get())};
    Transform[] transforms = new Transform[] {Transforms.identity("colA")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("colA = \"O'Hara\"", where);
  }

  @Test
  void testIdentityIntegerPartition() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("part", "7"));
    Column[] columns = new Column[] {column("part", Types.IntegerType.get())};
    Transform[] transforms = new Transform[] {Transforms.identity("part")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("part = 7", where);
  }

  @Test
  void testIdentityBooleanPartition() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("flag", "true"));
    Column[] columns = new Column[] {column("flag", Types.BooleanType.get())};
    Transform[] transforms = new Transform[] {Transforms.identity("flag")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("flag = true", where);
  }

  @Test
  void testIdentityStringPartitionEscapesQuotesAndBackslashes() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("colA", "a\"b\\c"));
    Column[] columns = new Column[] {column("colA", Types.StringType.get())};
    Transform[] transforms = new Transform[] {Transforms.identity("colA")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("colA = \"a\\\"b\\\\c\"", where);
  }

  @Test
  void testIdentityTimestampPartition() {
    List<PartitionEntry> partitions =
        Arrays.asList(new PartitionEntryImpl("ts", "2024-01-01 00:00:00"));
    Column[] columns = new Column[] {column("ts", Types.TimestampType.withTimeZone())};
    Transform[] transforms = new Transform[] {Transforms.identity("ts")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    // Double-quoted typed literal survives CALL where => '...' embedding.
    Assertions.assertEquals("ts = TIMESTAMP \"2024-01-01 00:00:00\"", where);
  }

  @Test
  void testIdentityTimePartitionUsesDoubleQuotedLiteral() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("t", "12:34:56"));
    Column[] columns = new Column[] {column("t", Types.TimeType.get())};
    Transform[] transforms = new Transform[] {Transforms.identity("t")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("t = TIME \"12:34:56\"", where);
  }

  @Test
  void testBucketPartitionUsesIcebergArgumentOrder() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("bucket", "14"));
    Column[] columns = new Column[] {column("user_id", Types.LongType.get())};
    Transform[] transforms = new Transform[] {Transforms.bucket(16, new String[] {"user_id"})};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("bucket(16, user_id) = 14", where);
  }

  @Test
  void testTruncatePartitionUsesIcebergArgumentOrder() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("trunc", "usa"));
    Column[] columns = new Column[] {column("country", Types.StringType.get())};
    Transform[] transforms = new Transform[] {Transforms.truncate(3, "country")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("truncate(3, country) = \"usa\"", where);
  }

  @Test
  void testBucketAndIdentityPartition() {
    List<PartitionEntry> partitions =
        Arrays.asList(new PartitionEntryImpl("colA", "abc"), new PartitionEntryImpl("bucket", "1"));
    Column[] columns =
        new Column[] {
          column("colA", Types.StringType.get()), column("colB", Types.IntegerType.get())
        };
    Transform[] transforms =
        new Transform[] {Transforms.identity("colA"), Transforms.bucket(2, new String[] {"colB"})};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("colA = \"abc\" AND bucket(2, colB) = 1", where);
  }

  @Test
  void testTruncateAndYearPartition() {
    List<PartitionEntry> partitions =
        Arrays.asList(
            new PartitionEntryImpl("truncate", "prefix"), new PartitionEntryImpl("year", "2024"));
    Column[] columns =
        new Column[] {
          column("colC", Types.StringType.get()),
          column("colD", Types.TimestampType.withoutTimeZone())
        };
    Transform[] transforms =
        new Transform[] {Transforms.truncate(5, "colC"), Transforms.year("colD")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("truncate(5, colC) = \"prefix\" AND years(colD) = 2024", where);
  }

  @Test
  void testDayPartitionUsesIcebergDaysFunctionWithDateLiteral() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("day", "19723"));
    Column[] columns = new Column[] {column("event_time", Types.TimestampType.withoutTimeZone())};
    Transform[] transforms = new Transform[] {Transforms.day("event_time")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    // epoch day 19723 == 2024-01-01; Iceberg Spark days() returns DateType
    Assertions.assertEquals("days(event_time) = DATE \"2024-01-01\"", where);
  }

  @Test
  void testDayPartitionOnNestedStructFieldResolvesTopLevelColumn() {
    // Real schemas expose struct column "event", while the transform path is event.ts.
    List<PartitionEntry> partitions =
        Arrays.asList(new PartitionEntryImpl("event.ts_day", "2026-03-18"));
    Column[] columns = new Column[] {eventStructColumn()};
    Transform[] transforms = new Transform[] {Transforms.day(new String[] {"event", "ts"})};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("days(event.ts) = DATE \"2026-03-18\"", where);
  }

  @Test
  void testYearPartitionOnNestedStructField() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("event.ts_year", "56"));
    Column[] columns = new Column[] {eventStructColumn()};
    Transform[] transforms = new Transform[] {Transforms.year(new String[] {"event", "ts"})};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("years(event.ts) = 56", where);
  }

  @Test
  void testMonthPartitionOnNestedStructField() {
    List<PartitionEntry> partitions =
        Arrays.asList(new PartitionEntryImpl("event.ts_month", "674"));
    Column[] columns = new Column[] {eventStructColumn()};
    Transform[] transforms = new Transform[] {Transforms.month(new String[] {"event", "ts"})};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("months(event.ts) = 674", where);
  }

  @Test
  void testHourPartitionOnNestedStructField() {
    List<PartitionEntry> partitions =
        Arrays.asList(new PartitionEntryImpl("event.ts_hour", "492724"));
    Column[] columns = new Column[] {eventStructColumn()};
    Transform[] transforms = new Transform[] {Transforms.hour(new String[] {"event", "ts"})};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("hours(event.ts) = 492724", where);
  }

  @Test
  void testBucketPartitionOnNestedStructField() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("event.id_bucket", "3"));
    Column[] columns =
        new Column[] {
          column(
              "event",
              Types.StructType.of(Types.StructType.Field.notNullField("id", Types.LongType.get())))
        };
    Transform[] transforms =
        new Transform[] {Transforms.bucket(8, new String[][] {new String[] {"event", "id"}})};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("bucket(8, event.id) = 3", where);
  }

  @Test
  void testTruncatePartitionOnNestedStructField() {
    List<PartitionEntry> partitions =
        Arrays.asList(new PartitionEntryImpl("event.loc_trunc", "ab"));
    Column[] columns =
        new Column[] {
          column(
              "event",
              Types.StructType.of(
                  Types.StructType.Field.notNullField("loc", Types.StringType.get())))
        };
    Transform[] transforms =
        new Transform[] {Transforms.truncate(2, new String[] {"event", "loc"})};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("truncate(2, event.loc) = \"ab\"", where);
  }

  @Test
  void testDayPartitionAcceptsIsoDateFromUpdateStatsPath() {
    // Production path: IcebergUpdateStatsAndMetricsJob stores Spark date via String.valueOf.
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("day", "2026-01-01"));
    Column[] columns = new Column[] {column("event_time", Types.TimestampType.withoutTimeZone())};
    Transform[] transforms = new Transform[] {Transforms.day("event_time")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("days(event_time) = DATE \"2026-01-01\"", where);
  }

  @Test
  void testDayPartitionRejectsNonIntegerNonIsoValue() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("day", "not-a-day"));
    Column[] columns = new Column[] {column("event_time", Types.TimestampType.withoutTimeZone())};
    Transform[] transforms = new Transform[] {Transforms.day("event_time")};

    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms));
    Assertions.assertTrue(ex.getMessage().contains("not-a-day"));
  }

  @Test
  void testMonthPartitionUsesIcebergMonthsFunction() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("month", "660"));
    Column[] columns = new Column[] {column("event_time", Types.TimestampType.withTimeZone())};
    Transform[] transforms = new Transform[] {Transforms.month("event_time")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("months(event_time) = 660", where);
  }

  @Test
  void testYearPartitionUsesIcebergYearsFunctionWithIntLiteral() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("year", "56"));
    Column[] columns = new Column[] {column("event_time", Types.TimestampType.withoutTimeZone())};
    Transform[] transforms = new Transform[] {Transforms.year("event_time")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("years(event_time) = 56", where);
  }

  @Test
  void testHourPartitionUsesIcebergHoursFunctionWithIntLiteral() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("hour", "492724"));
    Column[] columns = new Column[] {column("event_time", Types.TimestampType.withoutTimeZone())};
    Transform[] transforms = new Transform[] {Transforms.hour("event_time")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("hours(event_time) = 492724", where);
  }

  @Test
  void testWhereClauseCombinesMultiplePartitionsWithParenthesesForStringColumns() {
    Column[] columns =
        new Column[] {column("a", Types.StringType.get()), column("b", Types.StringType.get())};

    List<PartitionEntry> p1 =
        Arrays.asList(new PartitionEntryImpl("p1", "x1"), new PartitionEntryImpl("p1", "y1"));
    List<PartitionEntry> p2 =
        Arrays.asList(new PartitionEntryImpl("p2", "x2"), new PartitionEntryImpl("p2", "y2"));
    List<PartitionPath> partitions = Arrays.asList(PartitionPath.of(p1), PartitionPath.of(p2));

    Transform[] partitioning = new Transform[] {Transforms.identity("a"), Transforms.identity("b")};

    String where = PartitionUtils.getWhereClauseForPartitions(partitions, columns, partitioning);

    Assertions.assertEquals("(a = \"x1\" AND b = \"y1\") OR (a = \"x2\" AND b = \"y2\")", where);
  }

  @Test
  void testWhereClauseEmitsNumericLiteralsWithoutQuotes() {
    Column[] columns =
        new Column[] {column("id", Types.LongType.get()), column("score", Types.DoubleType.get())};

    List<PartitionEntry> p1 =
        Arrays.asList(new PartitionEntryImpl("p1", "123"), new PartitionEntryImpl("p1", "45.6"));
    List<PartitionEntry> p2 =
        Arrays.asList(new PartitionEntryImpl("p2", "456"), new PartitionEntryImpl("p2", "78.9"));
    List<PartitionPath> partitions = Arrays.asList(PartitionPath.of(p1), PartitionPath.of(p2));

    Transform[] partitioning =
        new Transform[] {Transforms.identity("id"), Transforms.identity("score")};

    String where = PartitionUtils.getWhereClauseForPartitions(partitions, columns, partitioning);

    Assertions.assertEquals("(id = 123 AND score = 45.6) OR (id = 456 AND score = 78.9)", where);
  }

  @Test
  void testIdentityOnDateColumnIsFormattedAsDateLiteral() {
    Column[] columns = new Column[] {column("dt", Types.DateType.get())};

    List<PartitionEntry> p1 = Arrays.asList(new PartitionEntryImpl("p", "2024-01-01"));
    List<PartitionPath> partitions = Arrays.asList(PartitionPath.of(p1));

    Transform[] partitioning = new Transform[] {Transforms.identity("dt")};

    String where = PartitionUtils.getWhereClauseForPartitions(partitions, columns, partitioning);

    Assertions.assertEquals("(dt = DATE \"2024-01-01\")", where);
  }

  @Test
  void testWhereClauseWithMixedTransformsAndMultiplePartitions() {
    Column[] columns =
        new Column[] {
          column("event.ts", Types.TimestampType.withoutTimeZone()),
          column("country", Types.StringType.get()),
          column("user_id", Types.LongType.get()),
          column("flag", Types.BooleanType.get())
        };

    Transform[] partitioning =
        new Transform[] {
          Transforms.hour(new String[] {"event", "ts"}),
          Transforms.truncate(3, "country"),
          Transforms.bucket(16, new String[] {"user_id"}),
          Transforms.identity("flag")
        };

    List<PartitionEntry> p1 =
        Arrays.asList(
            new PartitionEntryImpl("p", "8"),
            new PartitionEntryImpl("p", "usa"),
            new PartitionEntryImpl("p", "5"),
            new PartitionEntryImpl("p", "false"));
    List<PartitionEntry> p2 =
        Arrays.asList(
            new PartitionEntryImpl("p", "12"),
            new PartitionEntryImpl("p", "can"),
            new PartitionEntryImpl("p", "9"),
            new PartitionEntryImpl("p", "true"));
    List<PartitionPath> partitions = Arrays.asList(PartitionPath.of(p1), PartitionPath.of(p2));

    String where = PartitionUtils.getWhereClauseForPartitions(partitions, columns, partitioning);

    Assertions.assertEquals(
        "(hours(event.ts) = 8 AND truncate(3, country) = \"usa\" AND bucket(16, user_id) = 5 AND flag = false) "
            + "OR (hours(event.ts) = 12 AND truncate(3, country) = \"can\" AND bucket(16, user_id) = 9 AND flag = true)",
        where);
  }

  @Test
  void testNullPartitionsThrowsIllegalArgumentException() {
    Column[] columns = new Column[] {column("a", Types.StringType.get())};
    Transform[] partitioning = new Transform[] {Transforms.identity("a")};

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PartitionUtils.getWhereClauseForPartitions(null, columns, partitioning));
  }

  @Test
  void testEmptyColumnsThrowsIllegalArgumentException() {
    List<PartitionEntry> p1 = Arrays.asList(new PartitionEntryImpl("p", "v"));
    List<PartitionPath> partitions = Arrays.asList(PartitionPath.of(p1));
    Column[] columns = new Column[0];
    Transform[] partitioning = new Transform[] {Transforms.identity("p")};

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> PartitionUtils.getWhereClauseForPartitions(partitions, columns, partitioning));
  }
}
