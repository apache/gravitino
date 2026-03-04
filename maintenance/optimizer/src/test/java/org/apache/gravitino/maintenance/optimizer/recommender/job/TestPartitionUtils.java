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

  @Test
  void testIdentityStringPartition() {
    List<PartitionEntry> partitions = Arrays.asList(new PartitionEntryImpl("colA", "O'Hara"));
    Column[] columns = new Column[] {column("colA", Types.StringType.get())};
    Transform[] transforms = new Transform[] {Transforms.identity("colA")};

    String where = PartitionUtils.getWhereClauseForPartition(partitions, columns, transforms);

    Assertions.assertEquals("colA = \"O'Hara\"", where);
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

    Assertions.assertEquals("ts = TIMESTAMP '2024-01-01 00:00:00'", where);
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

    Assertions.assertEquals("colA = \"abc\" AND bucket(colB, 2) = 1", where);
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

    Assertions.assertEquals("truncate(colC, 5) = \"prefix\" AND year(colD) = 2024", where);
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

    Assertions.assertEquals("(dt = DATE '2024-01-01')", where);
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
        "(hour(event.ts) = 8 AND truncate(country, 3) = \"usa\" AND bucket(user_id, 16) = 5 AND flag = false) "
            + "OR (hour(event.ts) = 12 AND truncate(country, 3) = \"can\" AND bucket(user_id, 16) = 9 AND flag = true)",
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
