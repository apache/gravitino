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
package org.apache.gravitino.maintenance.jobs.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.apache.gravitino.job.JobTemplateProvider;
import org.apache.gravitino.job.SparkJobTemplate;
import org.junit.jupiter.api.Test;

public class TestSparkPiJob {

  @Test
  public void testJobTemplateHasCorrectName() {
    SparkPiJob job = new SparkPiJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template);
    assertEquals("builtin-sparkpi", template.name());
  }

  @Test
  public void testJobTemplateHasComment() {
    SparkPiJob job = new SparkPiJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.comment());
    assertFalse(template.comment().trim().isEmpty());
  }

  @Test
  public void testJobTemplateHasExecutable() {
    SparkPiJob job = new SparkPiJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.executable());
    assertFalse(template.executable().trim().isEmpty());
  }

  @Test
  public void testJobTemplateHasMainClass() {
    SparkPiJob job = new SparkPiJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.className());
    assertEquals(SparkPiJob.class.getName(), template.className());
  }

  @Test
  public void testJobTemplateHasArguments() {
    SparkPiJob job = new SparkPiJob();
    SparkJobTemplate template = job.jobTemplate();

    assertNotNull(template.arguments());
    assertEquals(1, template.arguments().size());
    assertEquals("{{slices}}", template.arguments().get(0));
  }

  @Test
  public void testJobTemplateHasSparkConfigs() {
    SparkPiJob job = new SparkPiJob();
    SparkJobTemplate template = job.jobTemplate();

    Map<String, String> configs = template.configs();
    assertNotNull(configs);
    assertFalse(configs.isEmpty());

    // Verify expected config keys with placeholders
    assertTrue(configs.containsKey("spark.master"));
    assertTrue(configs.containsKey("spark.executor.instances"));
    assertTrue(configs.containsKey("spark.executor.cores"));
    assertTrue(configs.containsKey("spark.executor.memory"));
    assertTrue(configs.containsKey("spark.driver.memory"));

    // Verify placeholders
    assertEquals("{{spark_master}}", configs.get("spark.master"));
    assertEquals("{{spark_executor_instances}}", configs.get("spark.executor.instances"));
    assertEquals("{{spark_executor_cores}}", configs.get("spark.executor.cores"));
    assertEquals("{{spark_executor_memory}}", configs.get("spark.executor.memory"));
    assertEquals("{{spark_driver_memory}}", configs.get("spark.driver.memory"));
  }

  @Test
  public void testJobTemplateHasVersion() {
    SparkPiJob job = new SparkPiJob();
    SparkJobTemplate template = job.jobTemplate();

    Map<String, String> customFields = template.customFields();
    assertNotNull(customFields);
    assertTrue(customFields.containsKey(JobTemplateProvider.PROPERTY_VERSION_KEY));

    String version = customFields.get(JobTemplateProvider.PROPERTY_VERSION_KEY);
    assertEquals("v1", version);
    assertTrue(version.matches(JobTemplateProvider.VERSION_VALUE_PATTERN));
  }

  @Test
  public void testJobTemplateNameMatchesBuiltInPattern() {
    SparkPiJob job = new SparkPiJob();
    SparkJobTemplate template = job.jobTemplate();

    assertTrue(template.name().matches(JobTemplateProvider.BUILTIN_NAME_PATTERN));
    assertTrue(template.name().startsWith(JobTemplateProvider.BUILTIN_NAME_PREFIX));
  }

  @Test
  public void testResolveExecutableReturnsNonEmptyPath() {
    SparkPiJob job = new SparkPiJob();
    String executable = job.jobTemplate().executable();

    assertNotNull(executable);
    assertFalse(executable.trim().isEmpty());
  }
}
