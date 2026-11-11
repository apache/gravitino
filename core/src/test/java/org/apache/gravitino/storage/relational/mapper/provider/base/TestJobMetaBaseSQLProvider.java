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
package org.apache.gravitino.storage.relational.mapper.provider.base;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJobMetaBaseSQLProvider {

  private final JobMetaBaseSQLProvider provider = new JobMetaBaseSQLProvider();

  @Test
  void testInsertJobMetaIncludesRuntimeJobTemplateColumn() {
    String sql = provider.insertJobMeta(null);

    Assertions.assertTrue(
        sql.contains("runtime_job_template"),
        () -> "Column list must include runtime_job_template, but got: " + sql);
    Assertions.assertTrue(
        sql.contains("#{jobMeta.runtimeJobTemplate}"),
        () -> "VALUES clause must bind runtimeJobTemplate, but got: " + sql);
  }

  @Test
  void testInsertJobMetaOnDuplicateKeyUpdateIncludesRuntimeJobTemplateColumn() {
    String sql = provider.insertJobMetaOnDuplicateKeyUpdate(null);
    String onDuplicateClause = sql.substring(sql.indexOf("ON DUPLICATE KEY UPDATE"));

    Assertions.assertTrue(
        sql.substring(0, sql.indexOf("VALUES")).contains("runtime_job_template"),
        () -> "Column list must include runtime_job_template, but got: " + sql);
    Assertions.assertTrue(
        sql.contains("#{jobMeta.runtimeJobTemplate}"),
        () -> "VALUES clause must bind runtimeJobTemplate, but got: " + sql);
    Assertions.assertTrue(
        onDuplicateClause.contains("runtime_job_template = #{jobMeta.runtimeJobTemplate}"),
        () ->
            "ON DUPLICATE KEY UPDATE must overwrite runtime_job_template, but got: "
                + onDuplicateClause);
  }

  @Test
  void testListJobPOsByMetalakeSelectsRuntimeJobTemplate() {
    assertSelectsRuntimeJobTemplate(provider.listJobPOsByMetalake("metalake"));
  }

  @Test
  void testListJobPOsByMetalakeAndTemplateSelectsRuntimeJobTemplate() {
    assertSelectsRuntimeJobTemplate(
        provider.listJobPOsByMetalakeAndTemplate("metalake", "template"));
  }

  @Test
  void testSelectJobPOByMetalakeAndRunIdSelectsRuntimeJobTemplate() {
    assertSelectsRuntimeJobTemplate(provider.selectJobPOByMetalakeAndRunId("metalake", 1L));
  }

  @Test
  void testBatchSelectJobByRunIdsSelectsRuntimeJobTemplate() {
    assertSelectsRuntimeJobTemplate(provider.batchSelectJobByRunIds("metalake", null));
  }

  private void assertSelectsRuntimeJobTemplate(String sql) {
    Assertions.assertTrue(
        sql.contains("jrm.runtime_job_template AS runtimeJobTemplate"),
        () -> "SELECT list must project runtime_job_template, but got: " + sql);
  }
}
