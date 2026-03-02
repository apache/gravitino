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
package org.apache.gravitino.maintenance.jobs.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.junit.jupiter.api.Test;

public class TestIcebergUpdateStatsJob {

  @Test
  public void testJobTemplateHasCorrectNameAndVersion() {
    IcebergUpdateStatsJob job = new IcebergUpdateStatsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template);
    assertEquals("builtin-iceberg-update-stats", template.name());
    assertTrue(template.name().matches(JobTemplateProvider.BUILTIN_NAME_PATTERN));
    assertEquals("v1", template.customFields().get(JobTemplateProvider.PROPERTY_VERSION_KEY));
  }

  @Test
  public void testJobTemplateArguments() {
    IcebergUpdateStatsJob job = new IcebergUpdateStatsJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.arguments());
    assertEquals(14, template.arguments().size());
    assertTrue(template.arguments().contains("--catalog"));
    assertTrue(template.arguments().contains("{{catalog_name}}"));
    assertTrue(template.arguments().contains("--table"));
    assertTrue(template.arguments().contains("{{table_identifier}}"));
    assertTrue(template.arguments().contains("--gravitino-uri"));
    assertTrue(template.arguments().contains("{{gravitino_uri}}"));
    assertTrue(template.arguments().contains("--metalake"));
    assertTrue(template.arguments().contains("{{metalake}}"));
    assertTrue(template.arguments().contains("--target-file-size-bytes"));
    assertTrue(template.arguments().contains("{{target_file_size_bytes}}"));
    assertTrue(template.arguments().contains("--statistics-updater"));
    assertTrue(template.arguments().contains("{{statistics_updater}}"));
  }

  @Test
  public void testParseArguments() {
    String[] args = {
      "--catalog", "cat",
      "--table", "db.tbl",
      "--gravitino-uri", "http://localhost:8090",
      "--metalake", "ml",
      "--target-file-size-bytes", "2048"
    };

    Map<String, String> parsed = IcebergUpdateStatsJob.parseArguments(args);
    assertEquals("cat", parsed.get("catalog"));
    assertEquals("db.tbl", parsed.get("table"));
    assertEquals("http://localhost:8090", parsed.get("gravitino-uri"));
    assertEquals("ml", parsed.get("metalake"));
    assertEquals("2048", parsed.get("target-file-size-bytes"));
  }

  @Test
  public void testBuildStatsSql() {
    String tableSql = IcebergUpdateStatsJob.buildTableStatsSql("cat", "db.tbl", 100000L);
    String partitionSql = IcebergUpdateStatsJob.buildPartitionStatsSql("cat", "db.tbl", 100000L);

    assertTrue(tableSql.contains("FROM cat.db.tbl.files"));
    assertFalse(tableSql.contains("GROUP BY partition"));
    assertTrue(tableSql.contains("AS datafile_mse"));
    assertTrue(partitionSql.contains("FROM cat.db.tbl.files"));
    assertTrue(partitionSql.contains("GROUP BY partition"));
    assertTrue(partitionSql.startsWith("SELECT partition"));
  }

  @Test
  public void testParseTargetFileSize() {
    assertEquals(100000L, IcebergUpdateStatsJob.parseTargetFileSize(null));
    assertEquals(100000L, IcebergUpdateStatsJob.parseTargetFileSize(""));
    assertEquals(2048L, IcebergUpdateStatsJob.parseTargetFileSize("2048"));
    assertThrows(
        IllegalArgumentException.class, () -> IcebergUpdateStatsJob.parseTargetFileSize("-1"));
    assertThrows(
        IllegalArgumentException.class, () -> IcebergUpdateStatsJob.parseTargetFileSize("abc"));
  }
}
