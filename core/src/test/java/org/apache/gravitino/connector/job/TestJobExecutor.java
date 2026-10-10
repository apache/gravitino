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

package org.apache.gravitino.connector.job;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.ShellJobTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestJobExecutor {

  private abstract static class BaseJobExecutor implements JobExecutor {

    @Override
    public void initialize(Map<String, String> configs) {}

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

  /** Implements only the deprecated submit method that receives local paths. */
  @SuppressWarnings("deprecation")
  private static class LocalPathJobExecutor extends BaseJobExecutor {

    private JobTemplate submitted;

    @Override
    public String submitJob(JobTemplate jobTemplate) {
      submitted = jobTemplate;
      return "execution-1";
    }
  }

  /** Implements only the submit method that receives the URIs, and returns the executable. */
  private static class UriJobExecutor extends BaseJobExecutor {

    @Override
    public String submitJob(JobContext context, JobTemplate jobTemplate) {
      return jobTemplate.executable();
    }
  }

  @TempDir private Path tempDir;

  @Test
  public void testSubmitJobWithContextLocalizesAndDelegates() throws IOException {
    File script = Files.createFile(tempDir.resolve("run.sh")).toFile();
    File stagingDir = Files.createDirectory(tempDir.resolve("staging")).toFile();
    ShellJobTemplate template =
        ShellJobTemplate.builder()
            .withName("shell_job")
            .withExecutable(script.toURI().toString())
            .withArguments(Lists.newArrayList("arg"))
            .build();

    LocalPathJobExecutor executor = new LocalPathJobExecutor();
    String executionId = executor.submitJob(new JobContext(1L, "metalake", stagingDir), template);

    Assertions.assertEquals("execution-1", executionId);
    // The job executor that only implements submitJob(JobTemplate) gets local paths in the
    // staging directory, as before.
    Assertions.assertEquals(
        new File(stagingDir, script.getName()).getAbsolutePath(), executor.submitted.executable());
    Assertions.assertEquals(template.arguments(), executor.submitted.arguments());
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testSubmitJobWithoutContextIsUnsupportedByDefault() {
    JobExecutor executor = new UriJobExecutor();
    ShellJobTemplate template =
        ShellJobTemplate.builder().withName("shell_job").withExecutable("/bin/echo").build();

    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> executor.submitJob(template));
    Assertions.assertEquals(
        "/bin/echo",
        executor.submitJob(new JobContext(1L, "metalake", tempDir.toFile()), template));
  }

  @Test
  public void testJobContextRejectsBlankMetalake() {
    File stagingDir = tempDir.toFile();
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new JobContext(1L, null, stagingDir));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> new JobContext(1L, " ", stagingDir));
    Assertions.assertThrows(IllegalArgumentException.class, () -> new JobContext(1L, "m", null));
  }
}
