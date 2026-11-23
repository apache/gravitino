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
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;
import org.apache.gravitino.maintenance.optimizer.common.util.IdentifierUtils;
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
    Map<String, String> conf =
        new GravitinoCompactionJobAdapter()
            .jobConfig(
                mockCompactionJobContext("binpack", ""), IdentifierUtils.DEFAULT_SCHEMA_SEPARATOR);
    Assertions.assertEquals("db.table", conf.get("table_identifier"));
    Assertions.assertEquals("binpack", conf.get("strategy"));
    Assertions.assertEquals("1g", conf.get("spark_executor_memory"));
    Assertions.assertEquals("", conf.get("catalog_type"));
    Assertions.assertTrue(conf.containsKey("spark_master"));
    Assertions.assertTrue(conf.containsKey("warehouse_location"));
  }

  @Test
  public void testSortStrategyAndSortOrder() {
    Map<String, String> conf =
        new GravitinoCompactionJobAdapter()
            .jobConfig(
                mockCompactionJobContext("sort", "zorder(c1,c2)"),
                IdentifierUtils.DEFAULT_SCHEMA_SEPARATOR);
    Assertions.assertEquals("zorder(c1,c2)", conf.get("sort_order"));
    Assertions.assertEquals("sort", conf.get("strategy"));
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
                    partitions),
                IdentifierUtils.DEFAULT_SCHEMA_SEPARATOR);

    Assertions.assertEquals(
        "(`catalog`.system.days(`ts`) = DATE '2026-01-01')", conf.get("where_clause"));
  }

  @Test
  public void testWhereClauseUsesSubmitterSparkCatalogWhenNamesDiffer() {
    // Gravitino table catalog "lake" vs Spark CALL catalog_name "ice" from jobSubmitterConfig.
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
    CompactionJobContext context =
        new CompactionJobContext(
            NameIdentifier.of("lake", "db", "events"),
            Map.of("target_file_size_bytes", "1073741824"),
            "compaction-job-template",
            "binpack",
            "",
            columns,
            partitioning,
            partitions);
    OptimizerConfig config =
        new OptimizerConfig(
            Map.of(OptimizerConfig.JOB_SUBMITTER_CONFIG_PREFIX + "catalog_name", "ice"));

    Map<String, String> conf =
        GravitinoJobSubmitter.buildJobConfig(
            config,
            context,
            new GravitinoCompactionJobAdapter(),
            IdentifierUtils.DEFAULT_SCHEMA_SEPARATOR);

    Assertions.assertEquals("ice", conf.get("catalog_name"));
    Assertions.assertEquals(
        "(`ice`.system.days(`ts`) = DATE '2026-01-01')", conf.get("where_clause"));
  }

  @Test
  public void testStructInStructHyphenatedFieldWhere() {
    Column[] columns =
        new Column[] {
          ColumnDTO.builder()
              .withName("event")
              .withDataType(
                  Types.StructType.of(
                      Types.StructType.Field.notNullField(
                          "meta",
                          Types.StructType.of(
                              Types.StructType.Field.notNullField(
                                  "user-id", Types.IntegerType.get())))))
              .build()
        };
    Transform[] partitioning =
        new Transform[] {Transforms.identity(new String[] {"event", "meta", "user-id"})};
    List<PartitionPath> partitions =
        List.of(PartitionPath.of(List.of(new PartitionEntryImpl("event.meta.user-id", "7"))));

    Map<String, String> conf =
        new GravitinoCompactionJobAdapter()
            .jobConfig(
                new CompactionJobContext(
                    NameIdentifier.of("catalog", "db", "events"),
                    Map.of("target_file_size_bytes", "1073741824"),
                    "compaction-job-template",
                    "binpack",
                    "",
                    columns,
                    partitioning,
                    partitions),
                IdentifierUtils.DEFAULT_SCHEMA_SEPARATOR);

    Assertions.assertEquals("(`event`.`meta`.`user-id` = 7)", conf.get("where_clause"));
  }

  /** #15048: hierarchical Gravitino schemas become Spark-physical table identifiers for CALL. */
  @Test
  public void testHierarchicalSchemaTableIdentifierIsSparkPhysical() {
    Map<String, String> conf =
        new GravitinoCompactionJobAdapter()
            .jobConfig(
                new CompactionJobContext(
                    NameIdentifier.of("catalog", "a:b:c", "events"),
                    Map.of("target_file_size_bytes", "1073741824"),
                    "compaction-job-template",
                    "binpack",
                    "",
                    new Column[0],
                    new Transform[0],
                    List.of()),
                IdentifierUtils.DEFAULT_SCHEMA_SEPARATOR);

    Assertions.assertEquals("a.b.c.events", conf.get("table_identifier"));
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
