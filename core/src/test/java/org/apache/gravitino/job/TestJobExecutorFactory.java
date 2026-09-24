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
package org.apache.gravitino.job;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.connector.job.JobExecutionInfo;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.local.LocalJobExecutor;
import org.apache.gravitino.job.local.LocalJobExecutorConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestJobExecutorFactory {

  private static final String OUTPUT_INDEX_DIR_NAME = ".job-output-index";

  private File testDir;

  private File stagingDir;

  private Config config;

  @BeforeEach
  public void setUp() throws IOException {
    testDir = Files.createTempDirectory("gravitino-test-job-executor-factory").toFile();
    stagingDir = new File(testDir, "staging");
    config = new Config(false) {};
    config.set(Configs.JOB_STAGING_DIR, stagingDir.getAbsolutePath());
  }

  @AfterEach
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(testDir);
  }

  @Test
  public void testLocalJobExecutorUsesJobStagingDir() throws IOException {
    try (JobExecutor executor = JobExecutorFactory.create(config)) {
      Assertions.assertInstanceOf(LocalJobExecutor.class, executor);
      Assertions.assertTrue(new File(stagingDir, OUTPUT_INDEX_DIR_NAME).isDirectory());
    }
  }

  @Test
  public void testLocalJobExecutorIgnoresConfiguredStagingDir() throws IOException {
    File otherDir = new File(testDir, "other");
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.jobExecutor.local." + LocalJobExecutorConfigs.STAGING_DIR,
            otherDir.getAbsolutePath()),
        key -> true);

    try (JobExecutor executor = JobExecutorFactory.create(config)) {
      Assertions.assertTrue(new File(stagingDir, OUTPUT_INDEX_DIR_NAME).isDirectory());
      Assertions.assertFalse(otherDir.exists());
    }
  }

  @Test
  public void testLocalJobExecutorUnderCustomNameUsesJobStagingDir() throws IOException {
    config.set(Configs.JOB_EXECUTOR, "mylocal");
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.jobExecutor.mylocal.class", LocalJobExecutor.class.getCanonicalName()),
        key -> true);

    try (JobExecutor executor = JobExecutorFactory.create(config)) {
      Assertions.assertInstanceOf(LocalJobExecutor.class, executor);
      Assertions.assertTrue(new File(stagingDir, OUTPUT_INDEX_DIR_NAME).isDirectory());
    }
  }

  @Test
  public void testLocalJobExecutorSubclassUsesJobStagingDir() throws IOException {
    config.set(Configs.JOB_EXECUTOR, "custom");
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.jobExecutor.custom.class", CustomLocalJobExecutor.class.getName()),
        key -> true);

    try (JobExecutor executor = JobExecutorFactory.create(config)) {
      Assertions.assertInstanceOf(CustomLocalJobExecutor.class, executor);
      Assertions.assertTrue(new File(stagingDir, OUTPUT_INDEX_DIR_NAME).isDirectory());
    }
  }

  @Test
  public void testCustomJobExecutorConfigsAreUnchanged() throws IOException {
    config.set(Configs.JOB_EXECUTOR, "recording");
    config.loadFromMap(
        ImmutableMap.of(
            "gravitino.jobExecutor.recording.class",
            RecordingJobExecutor.class.getName(),
            "gravitino.jobExecutor.recording.foo",
            "bar"),
        key -> true);

    try (JobExecutor executor = JobExecutorFactory.create(config)) {
      Assertions.assertInstanceOf(RecordingJobExecutor.class, executor);
      Map<String, String> configs = ((RecordingJobExecutor) executor).configs;
      Assertions.assertEquals("bar", configs.get("foo"));
      Assertions.assertFalse(configs.containsKey(LocalJobExecutorConfigs.STAGING_DIR));
    }
  }

  /** A user's subclass of the local job executor, inheriting its initialization. */
  public static class CustomLocalJobExecutor extends LocalJobExecutor {}

  /** A job executor that only records the configurations it's initialized with. */
  public static class RecordingJobExecutor implements JobExecutor {

    private Map<String, String> configs;

    @Override
    public void initialize(Map<String, String> configs) {
      this.configs = configs;
    }

    @Override
    public String submitJob(JobTemplate jobTemplate) {
      throw new UnsupportedOperationException();
    }

    @Override
    public JobExecutionInfo getJobExecutionInfo(String jobId) throws NoSuchJobException {
      throw new NoSuchJobException("No job found with ID: %s", jobId);
    }

    @Override
    public void cancelJob(String jobId) throws NoSuchJobException {
      throw new NoSuchJobException("No job found with ID: %s", jobId);
    }

    @Override
    public void close() {}
  }
}
