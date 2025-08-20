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
package org.apache.gravitino.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.job.JobDTO;
import org.apache.gravitino.dto.job.JobTemplateDTO;
import org.apache.gravitino.dto.job.ShellJobTemplateDTO;
import org.apache.gravitino.dto.job.SparkJobTemplateDTO;
import org.apache.gravitino.dto.requests.JobRunRequest;
import org.apache.gravitino.dto.requests.JobTemplateRegisterRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.JobListResponse;
import org.apache.gravitino.dto.responses.JobResponse;
import org.apache.gravitino.dto.responses.JobTemplateListResponse;
import org.apache.gravitino.dto.responses.JobTemplateResponse;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobTemplate;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSupportsJobs extends TestBase {

  private static final String METALAKE_FOR_JOB_TEST = "metalake_for_job_test";

  private static GravitinoMetalake metalake;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
    metalake = TestGravitinoMetalake.createMetalake(client, METALAKE_FOR_JOB_TEST);
  }

  @Test
  public void testListJobTemplates() throws JsonProcessingException {
    JobTemplateDTO template1 = newShellJobTemplateDTO("shell-job-template");
    JobTemplateDTO template2 = newSparkJobTemplateDTO("spark-job-template");
    List<JobTemplateDTO> templates = Lists.newArrayList(template1, template2);
    JobTemplateListResponse resp = new JobTemplateListResponse(templates);

    buildMockResource(Method.GET, jobTemplatesPath(), null, resp, HttpStatus.SC_OK);

    List<JobTemplate> jobTemplates = metalake.listJobTemplates();
    Assertions.assertEquals(2, jobTemplates.size());

    List<JobTemplate> expected =
        templates.stream()
            .map(org.apache.gravitino.dto.util.DTOConverters::fromDTO)
            .collect(Collectors.toList());

    Assertions.assertEquals(expected, jobTemplates);

    // Test throw NoSuchMetalakeException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, jobTemplatesPath(), null, errorResp, HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(NoSuchMetalakeException.class, () -> metalake.listJobTemplates());

    // Test throw MetalakeNotInUseException
    ErrorResponse errorResp2 =
        ErrorResponse.notInUse(
            MetalakeNotInUseException.class.getSimpleName(),
            "mock error",
            new MetalakeNotInUseException("mock error"));
    buildMockResource(Method.GET, jobTemplatesPath(), null, errorResp2, HttpStatus.SC_CONFLICT);
    Assertions.assertThrows(MetalakeNotInUseException.class, () -> metalake.listJobTemplates());

    // Test throw RuntimeException
    ErrorResponse errorResp3 = ErrorResponse.internalError("mock error");
    buildMockResource(
        Method.GET, jobTemplatesPath(), null, errorResp3, HttpStatus.SC_INTERNAL_SERVER_ERROR);
    Assertions.assertThrows(RuntimeException.class, () -> metalake.listJobTemplates());
  }

  @Test
  public void testRegisterJobTemplate() throws JsonProcessingException {
    JobTemplateDTO templateDTO = newShellJobTemplateDTO("shell-job-template");
    JobTemplate template = org.apache.gravitino.dto.util.DTOConverters.fromDTO(templateDTO);
    JobTemplateRegisterRequest req = new JobTemplateRegisterRequest(templateDTO);

    BaseResponse resp = new BaseResponse();
    buildMockResource(Method.POST, jobTemplatesPath(), req, resp, HttpStatus.SC_OK);

    Assertions.assertDoesNotThrow(() -> metalake.registerJobTemplate(template));

    // Test throw JobTemplateAlreadyExistsException
    ErrorResponse errorResp =
        ErrorResponse.alreadyExists(
            JobTemplateAlreadyExistsException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, jobTemplatesPath(), req, errorResp, HttpStatus.SC_CONFLICT);

    Assertions.assertThrows(
        JobTemplateAlreadyExistsException.class, () -> metalake.registerJobTemplate(template));
  }

  @Test
  public void testGetJobTemplate() throws JsonProcessingException {
    String jobTemplateName = "shell-job-template";
    JobTemplateDTO templateDTO = newShellJobTemplateDTO(jobTemplateName);
    JobTemplate expected = org.apache.gravitino.dto.util.DTOConverters.fromDTO(templateDTO);
    JobTemplateResponse resp = new JobTemplateResponse(templateDTO);

    buildMockResource(
        Method.GET, jobTemplatesPath() + "/" + jobTemplateName, null, resp, HttpStatus.SC_OK);

    JobTemplate actual = metalake.getJobTemplate(jobTemplateName);
    Assertions.assertEquals(expected, actual);

    // Test throw NoSuchJobTemplateException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchJobTemplateException.class.getSimpleName(), "mock error");
    buildMockResource(
        Method.GET,
        jobTemplatesPath() + "/" + jobTemplateName,
        null,
        errorResp,
        HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchJobTemplateException.class, () -> metalake.getJobTemplate(jobTemplateName));
  }

  @Test
  public void testDeleteJobTemplate() throws JsonProcessingException {
    String jobTemplateName = "shell-job-template";
    DropResponse resp = new DropResponse(true);

    buildMockResource(
        Method.DELETE, jobTemplatesPath() + "/" + jobTemplateName, null, resp, HttpStatus.SC_OK);

    Assertions.assertTrue(metalake.deleteJobTemplate(jobTemplateName));

    // Test throw InUseException
    ErrorResponse errorResp2 =
        ErrorResponse.inUse(
            InUseException.class.getSimpleName(), "mock error", new InUseException("mock error"));
    buildMockResource(
        Method.DELETE,
        jobTemplatesPath() + "/" + jobTemplateName,
        null,
        errorResp2,
        HttpStatus.SC_CONFLICT);
    Assertions.assertThrows(
        InUseException.class, () -> metalake.deleteJobTemplate(jobTemplateName));
  }

  @Test
  public void testListJobs() throws JsonProcessingException {
    String jobTemplateName = "shell-job-template";
    String jobId1 = "job-1";
    String jobId2 = "job-2";

    List<JobDTO> jobs =
        Lists.newArrayList(newJobDTO(jobId1, jobTemplateName), newJobDTO(jobId2, jobTemplateName));

    JobListResponse resp = new JobListResponse(jobs);

    buildMockResource(Method.GET, jobRunsPath(), null, resp, HttpStatus.SC_OK);

    List<JobHandle> actualJobs = metalake.listJobs();
    Assertions.assertEquals(2, actualJobs.size());
    compare(jobs.get(0), actualJobs.get(0));
    compare(jobs.get(1), actualJobs.get(1));

    // Test throw NoSuchJobTemplateException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchJobTemplateException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, jobRunsPath(), null, errorResp, HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchJobTemplateException.class, () -> metalake.listJobs(jobTemplateName));

    // Test list jobs by job template name
    buildMockResource(
        Method.GET,
        jobRunsPath(),
        ImmutableMap.of("jobTemplateName", jobTemplateName),
        null,
        resp,
        HttpStatus.SC_OK);

    List<JobHandle> jobsByTemplate = metalake.listJobs(jobTemplateName);
    Assertions.assertEquals(2, jobsByTemplate.size());
    compare(jobs.get(0), jobsByTemplate.get(0));
    compare(jobs.get(1), jobsByTemplate.get(1));
  }

  @Test
  public void testGetJob() throws JsonProcessingException {
    String jobId = "job-1";
    String jobTemplateName = "shell-job-template";
    JobDTO expectedJob = newJobDTO(jobId, jobTemplateName);
    JobResponse resp = new JobResponse(expectedJob);

    buildMockResource(Method.GET, jobRunsPath() + "/" + jobId, null, resp, HttpStatus.SC_OK);

    JobHandle actualHandle = metalake.getJob(jobId);
    compare(expectedJob, actualHandle);

    // Test throw NoSuchJobTemplateException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchJobException.class.getSimpleName(), "mock error");
    buildMockResource(
        Method.GET, jobRunsPath() + "/" + jobId, null, errorResp, HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(NoSuchJobException.class, () -> metalake.getJob(jobId));
  }

  @Test
  public void testRunJob() throws JsonProcessingException {
    String jobTemplateName = "shell-job-template";
    String jobId = "job-1";
    JobDTO expectedJob = newJobDTO(jobId, jobTemplateName);
    JobResponse resp = new JobResponse(expectedJob);
    JobRunRequest req = new JobRunRequest(jobTemplateName, ImmutableMap.of());

    buildMockResource(Method.POST, jobRunsPath(), req, resp, HttpStatus.SC_OK);

    JobHandle actualHandle = metalake.runJob(jobTemplateName, ImmutableMap.of());
    compare(expectedJob, actualHandle);

    // Test throw NoSuchJobTemplateException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchJobTemplateException.class.getSimpleName(), "mock error");
    buildMockResource(Method.POST, jobRunsPath(), req, errorResp, HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(
        NoSuchJobTemplateException.class,
        () -> metalake.runJob(jobTemplateName, ImmutableMap.of()));
  }

  @Test
  public void testCancelJob() throws JsonProcessingException {
    String jobId = "job-1";
    String jobTemplateName = "shell-job-template";
    JobDTO expectedJob = newJobDTO(jobId, jobTemplateName);
    JobResponse resp = new JobResponse(expectedJob);

    buildMockResource(Method.POST, jobRunsPath() + "/" + jobId, null, resp, HttpStatus.SC_OK);

    JobHandle actualHandle = metalake.cancelJob(jobId);
    compare(expectedJob, actualHandle);

    // Test throw NoSuchJobException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchJobException.class.getSimpleName(), "mock error");
    buildMockResource(
        Method.POST, jobRunsPath() + "/" + jobId, null, errorResp, HttpStatus.SC_NOT_FOUND);
    Assertions.assertThrows(NoSuchJobException.class, () -> metalake.cancelJob(jobId));
  }

  private void compare(JobDTO expected, JobHandle actual) {
    Assertions.assertEquals(expected.jobId(), actual.jobId());
    Assertions.assertEquals(expected.jobTemplateName(), actual.jobTemplateName());
    Assertions.assertEquals(expected.status(), actual.jobStatus());
  }

  private String jobTemplatesPath() {
    return "/api/metalakes/" + METALAKE_FOR_JOB_TEST + "/jobs/templates";
  }

  private String jobRunsPath() {
    return "/api/metalakes/" + METALAKE_FOR_JOB_TEST + "/jobs/runs";
  }

  private JobTemplateDTO newShellJobTemplateDTO(String name) {
    return ShellJobTemplateDTO.builder()
        .withJobType(JobTemplate.JobType.SHELL)
        .withName(name)
        .withComment("This is a shell job template")
        .withExecutable("/path/to/script.sh")
        .withArguments(Collections.emptyList())
        .withEnvironments(Collections.emptyMap())
        .withCustomFields(Collections.emptyMap())
        .withScripts(Collections.emptyList())
        .build();
  }

  private JobTemplateDTO newSparkJobTemplateDTO(String name) {
    return SparkJobTemplateDTO.builder()
        .withJobType(JobTemplate.JobType.SPARK)
        .withName(name)
        .withComment("This is a spark job template")
        .withExecutable("/path/to/spark-job.jar")
        .withArguments(Collections.emptyList())
        .withEnvironments(Collections.emptyMap())
        .withCustomFields(Collections.emptyMap())
        .withClassName("org.example.SparkJob")
        .withJars(Collections.emptyList())
        .withFiles(Collections.emptyList())
        .withArchives(Collections.emptyList())
        .withConfigs(Collections.emptyMap())
        .build();
  }

  private JobDTO newJobDTO(String jobId, String templateName) {
    return new JobDTO(
        jobId,
        templateName,
        JobHandle.Status.QUEUED,
        AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build());
  }
}
