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

import java.util.List;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.maintenance.optimizer.api.common.PartitionPath;
import org.apache.gravitino.maintenance.optimizer.common.PartitionEntryImpl;
import org.apache.gravitino.maintenance.optimizer.recommender.handler.compaction.CompactionJobContext;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoCompactionJobAdapter {

  @Test
  public void testJobTemplateName() {
    GravitinoCompactionJobAdapter jobAdapter = new GravitinoCompactionJobAdapter();
    Assertions.assertEquals(
        Map.of(
            "table_identifier", "db.table",
            "where_clause", "",
            "sort_order", "",
            "strategy", "binpack",
            "options", "{\"target_file_size_bytes\":\"1073741824\"}"),
        jobAdapter.jobConfig(mockCompactionJobContext("binpack", "")));
  }

  @Test
  public void testSortStrategyAndSortOrder() {
    GravitinoCompactionJobAdapter jobAdapter = new GravitinoCompactionJobAdapter();
    Assertions.assertEquals(
        Map.of(
            "table_identifier", "db.table",
            "where_clause", "",
            "sort_order", "zorder(c1,c2)",
            "strategy", "sort",
            "options", "{\"target_file_size_bytes\":\"1073741824\"}"),
        jobAdapter.jobConfig(mockCompactionJobContext("sort", "zorder(c1,c2)")));
  }

  @Test
  public void testWhereClauseForDayPartitionUsesIsoDateFromStatsPath() {
    // Mirrors CLI submit-strategy-jobs: update-stats stores day as ISO date, adapter builds where.
    Column[] columns =
        new Column[] {
          ColumnDTO.builder()
              .withName("ts")
              .withDataType(Types.TimestampType.withoutTimeZone())
              .build()
        };
    Transform[] partitioning = new Transform[] {Transforms.day("ts")};
    List<PartitionPath> partitions =
        List.of(PartitionPath.of(List.of(new PartitionEntryImpl("ts_day", "2026-01-01"))));

    Map<String, String> conf =
        new GravitinoCompactionJobAdapter()
            .jobConfig(
                new CompactionJobContext(
                    NameIdentifier.of("catalog", "db", "table"),
                    Map.of("target_file_size_bytes", "1073741824"),
                    "compaction-job-template",
                    "binpack",
                    "",
                    columns,
                    partitioning,
                    partitions));

    Assertions.assertEquals("(days(ts) = DATE \"2026-01-01\")", conf.get("where_clause"));
  }

  private CompactionJobContext mockCompactionJobContext(String rewriteStrategy, String sortOrder) {
    String jobTemplateName = "compaction-job-template";
    Column[] columns = new Column[0];
    Transform[] partitioning = new Transform[0];
    Map<String, String> jobOptions = Map.of("target_file_size_bytes", "1073741824");
    return new CompactionJobContext(
        NameIdentifier.of("catalog", "db", "table"),
        jobOptions,
        jobTemplateName,
        rewriteStrategy,
        sortOrder,
        columns,
        partitioning,
        List.of());
  }
}
