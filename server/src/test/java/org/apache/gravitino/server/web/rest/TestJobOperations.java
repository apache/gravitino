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
package org.apache.gravitino.server.web.rest;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.dto.job.JobTemplateDTO;
import org.apache.gravitino.dto.requests.JobRunRequest;
import org.apache.gravitino.dto.requests.JobTemplateRegisterRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.JobListResponse;
import org.apache.gravitino.dto.responses.JobResponse;
import org.apache.gravitino.dto.responses.JobTemplateListResponse;
import org.apache.gravitino.dto.responses.JobTemplateResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.job.SparkJobTemplate;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.utils.NamespaceUtil;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestJobOperations extends JerseyTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {

    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private final JobOperationDispatcher jobOperationDispatcher = mock(JobOperationDispatcher.class);

  private final String metalake = "test_metalake";

  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test_user").withCreateTime(Instant.now()).build();

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(JobOperations.class);
    resourceConfig.register(ObjectMapperProvider.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(jobOperationDispatcher).to(JobOperationDispatcher.class).ranked(2);
            bindFactory(TestJobOperations.MockServletRequestFactory.class)
                .to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    IdGenerator idGenerator = new RandomIdGenerator();
    FieldUtils.writeField(GravitinoEnv.getInstance(), "idGenerator", idGenerator, true);
  }

  @Test
  public void testListJobTemplates() {
    JobTemplateEntity template1 =
        newShellJobTemplateEntity("shell_template_1", "Test Shell Template 1");
    JobTemplateEntity template2 =
        newSparkJobTemplateEntity("spark_template_1", "Test Spark Template 1");

    when(jobOperationDispatcher.listJobTemplates(metalake))
        .thenReturn(Lists.newArrayList(template1, template2));

    Response resp =
        target(jobTemplatePath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(APPLICATION_JSON_TYPE, resp.getMediaType());

    NameListResponse nameListResponse = resp.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, nameListResponse.getCode());
    String[] expectedNames = {template1.name(), template2.name()};
    Assertions.assertArrayEquals(expectedNames, nameListResponse.getNames());

    // Test list details
    Response resp1 =
        target(jobTemplatePath())
            .queryParam("details", "true")
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(APPLICATION_JSON_TYPE, resp1.getMediaType());

    JobTemplateListResponse jobTemplateListResponse =
        resp1.readEntity(JobTemplateListResponse.class);
    Assertions.assertEquals(0, jobTemplateListResponse.getCode());

    Assertions.assertEquals(2, jobTemplateListResponse.getJobTemplates().size());
    Assertions.assertEquals(
        JobOperations.toDTO(template1), jobTemplateListResponse.getJobTemplates().get(0));
    Assertions.assertEquals(
        JobOperations.toDTO(template2), jobTemplateListResponse.getJobTemplates().get(1));

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(jobOperationDispatcher)
        .listJobTemplates(metalake);

    Response resp2 =
        target(jobTemplatePath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResp.getType());

    // Test throw MetalakeNotInUseException
    doThrow(new MetalakeNotInUseException("mock error"))
        .when(jobOperationDispatcher)
        .listJobTemplates(metalake);

    Response resp3 =
        target(jobTemplatePath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_IN_USE_CODE, errorResp2.getCode());
    Assertions.assertEquals(MetalakeNotInUseException.class.getSimpleName(), errorResp2.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(jobOperationDispatcher)
        .listJobTemplates(metalake);

    Response resp4 =
        target(jobTemplatePath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());
    ErrorResponse errorResp3 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp3.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp3.getType());
  }

  @Test
  public void testRegisterJobTemplate() {
    JobTemplateEntity template =
        newShellJobTemplateEntity("shell_template_1", "Test Shell Template 1");
    JobTemplateDTO templateDTO = JobOperations.toDTO(template);
    JobTemplateRegisterRequest request = new JobTemplateRegisterRequest(templateDTO);

    doNothing().when(jobOperationDispatcher).registerJobTemplate(metalake, template);

    Response resp =
        target(jobTemplatePath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.json(request));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(APPLICATION_JSON_TYPE, resp.getMediaType());

    BaseResponse baseResp = resp.readEntity(BaseResponse.class);
    Assertions.assertEquals(0, baseResp.getCode());

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(jobOperationDispatcher)
        .registerJobTemplate(any(), any());

    Response resp2 =
        target(jobTemplatePath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResp.getType());

    // Test throw MetalakeNotInUseException
    doThrow(new MetalakeNotInUseException("mock error"))
        .when(jobOperationDispatcher)
        .registerJobTemplate(any(), any());

    Response resp3 =
        target(jobTemplatePath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_IN_USE_CODE, errorResp2.getCode());
    Assertions.assertEquals(MetalakeNotInUseException.class.getSimpleName(), errorResp2.getType());

    // Test throw JobTemplateAlreadyExistsException
    doThrow(new JobTemplateAlreadyExistsException("mock error"))
        .when(jobOperationDispatcher)
        .registerJobTemplate(any(), any());
    Response resp4 =
        target(jobTemplatePath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp4.getStatus());
    ErrorResponse errorResp3 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp3.getCode());
    Assertions.assertEquals(
        JobTemplateAlreadyExistsException.class.getSimpleName(), errorResp3.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(jobOperationDispatcher)
        .registerJobTemplate(any(), any());

    Response resp5 =
        target(jobTemplatePath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp5.getStatus());

    ErrorResponse errorResp4 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp4.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp4.getType());
  }

  @Test
  public void testGetJobTemplate() {
    JobTemplateEntity template =
        newShellJobTemplateEntity("shell_template_1", "Test Shell Template 1");

    when(jobOperationDispatcher.getJobTemplate(metalake, template.name())).thenReturn(template);

    Response resp =
        target(jobTemplatePath())
            .path(template.name())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(APPLICATION_JSON_TYPE, resp.getMediaType());

    JobTemplateResponse jobTemplateResp = resp.readEntity(JobTemplateResponse.class);
    Assertions.assertEquals(JobOperations.toDTO(template), jobTemplateResp.getJobTemplate());

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(jobOperationDispatcher)
        .getJobTemplate(any(), any());

    Response resp2 =
        target(jobTemplatePath())
            .path(template.name())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResp.getType());

    // Test throw MetalakeNotInUseException
    doThrow(new MetalakeNotInUseException("mock error"))
        .when(jobOperationDispatcher)
        .getJobTemplate(any(), any());

    Response resp3 =
        target(jobTemplatePath())
            .path(template.name())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_IN_USE_CODE, errorResp2.getCode());
    Assertions.assertEquals(MetalakeNotInUseException.class.getSimpleName(), errorResp2.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(jobOperationDispatcher)
        .getJobTemplate(any(), any());

    Response resp4 =
        target(jobTemplatePath())
            .path(template.name())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());
    ErrorResponse errorResp3 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp3.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp3.getType());

    // Test throw NoSuchJobTemplateException
    doThrow(new NoSuchJobTemplateException("mock error"))
        .when(jobOperationDispatcher)
        .getJobTemplate(any(), any());

    Response resp5 =
        target(jobTemplatePath())
            .path(template.name())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp5.getStatus());
    ErrorResponse errorResp4 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp4.getCode());
    Assertions.assertEquals(NoSuchJobTemplateException.class.getSimpleName(), errorResp4.getType());
  }

  @Test
  public void testDeleteJobTemplate() {
    String templateName = "shell_template_1";

    when(jobOperationDispatcher.deleteJobTemplate(metalake, templateName)).thenReturn(true);

    Response resp =
        target(jobTemplatePath())
            .path(templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(APPLICATION_JSON_TYPE, resp.getMediaType());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());

    Assertions.assertTrue(dropResp.dropped());

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(jobOperationDispatcher)
        .deleteJobTemplate(any(), any());

    Response resp2 =
        target(jobTemplatePath())
            .path(templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResp.getType());

    // Test throw MetalakeNotInUseException
    doThrow(new MetalakeNotInUseException("mock error"))
        .when(jobOperationDispatcher)
        .deleteJobTemplate(any(), any());

    Response resp3 =
        target(jobTemplatePath())
            .path(templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_IN_USE_CODE, errorResp2.getCode());
    Assertions.assertEquals(MetalakeNotInUseException.class.getSimpleName(), errorResp2.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(jobOperationDispatcher)
        .deleteJobTemplate(any(), any());

    Response resp4 =
        target(jobTemplatePath())
            .path(templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());
    ErrorResponse errorResp3 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp3.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp3.getType());

    // Test throw InUseException
    doThrow(new InUseException("mock error"))
        .when(jobOperationDispatcher)
        .deleteJobTemplate(any(), any());

    Response resp5 =
        target(jobTemplatePath())
            .path(templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp5.getStatus());

    ErrorResponse errorResp4 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.IN_USE_CODE, errorResp4.getCode());
    Assertions.assertEquals(InUseException.class.getSimpleName(), errorResp4.getType());
  }

  @Test
  public void testListJobs() {
    String templateName = "shell_template_1";
    JobEntity job1 = newJobEntity(templateName, JobHandle.Status.QUEUED);
    JobEntity job2 = newJobEntity(templateName, JobHandle.Status.STARTED);
    JobEntity job3 = newJobEntity("spark_template_1", JobHandle.Status.SUCCEEDED);

    when(jobOperationDispatcher.listJobs(metalake, Optional.empty()))
        .thenReturn(Lists.newArrayList(job1, job2, job3));

    Response resp =
        target(jobRunPath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(APPLICATION_JSON_TYPE, resp.getMediaType());

    JobListResponse jobListResponse = resp.readEntity(JobListResponse.class);
    Assertions.assertEquals(0, jobListResponse.getCode());

    Assertions.assertEquals(3, jobListResponse.getJobs().size());
    Assertions.assertEquals(JobOperations.toDTO(job1), jobListResponse.getJobs().get(0));
    Assertions.assertEquals(JobOperations.toDTO(job2), jobListResponse.getJobs().get(1));
    Assertions.assertEquals(JobOperations.toDTO(job3), jobListResponse.getJobs().get(2));

    // Test list jobs by template name
    when(jobOperationDispatcher.listJobs(metalake, Optional.of(templateName)))
        .thenReturn(Lists.newArrayList(job1, job2));

    Response resp1 =
        target(jobRunPath())
            .queryParam("jobTemplateName", templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(APPLICATION_JSON_TYPE, resp1.getMediaType());

    JobListResponse jobListResponse1 = resp1.readEntity(JobListResponse.class);
    Assertions.assertEquals(0, jobListResponse1.getCode());
    Assertions.assertEquals(2, jobListResponse1.getJobs().size());
    Assertions.assertEquals(JobOperations.toDTO(job1), jobListResponse1.getJobs().get(0));
    Assertions.assertEquals(JobOperations.toDTO(job2), jobListResponse1.getJobs().get(1));

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(jobOperationDispatcher)
        .listJobs(any(), any());

    Response resp2 =
        target(jobRunPath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResp.getType());

    // Test throw MetalakeNotInUseException
    doThrow(new MetalakeNotInUseException("mock error"))
        .when(jobOperationDispatcher)
        .listJobs(any(), any());

    Response resp3 =
        target(jobRunPath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_IN_USE_CODE, errorResp2.getCode());
    Assertions.assertEquals(MetalakeNotInUseException.class.getSimpleName(), errorResp2.getType());

    // Test NoSuchJobTemplateException
    doThrow(new NoSuchJobTemplateException("mock error"))
        .when(jobOperationDispatcher)
        .listJobs(any(), any());

    Response resp4 =
        target(jobRunPath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp4.getStatus());

    ErrorResponse errorResp3 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp3.getCode());
    Assertions.assertEquals(NoSuchJobTemplateException.class.getSimpleName(), errorResp3.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(jobOperationDispatcher).listJobs(any(), any());

    Response resp5 =
        target(jobRunPath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp5.getStatus());

    ErrorResponse errorResp4 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp4.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp4.getType());
  }

  @Test
  public void testRunJob() {
    String templateName = "shell_template_1";
    Map<String, String> jobConf = ImmutableMap.of("key1", "value1", "key2", "value2");
    JobEntity job = newJobEntity(templateName, JobHandle.Status.QUEUED);
    JobRunRequest req = new JobRunRequest(templateName, jobConf);

    when(jobOperationDispatcher.runJob(metalake, templateName, jobConf)).thenReturn(job);

    Response resp =
        target(jobRunPath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(APPLICATION_JSON_TYPE, resp.getMediaType());

    JobResponse jobResp = resp.readEntity(JobResponse.class);
    Assertions.assertEquals(0, jobResp.getCode());
    Assertions.assertEquals(JobOperations.toDTO(job), jobResp.getJob());

    // Test throw NoSuchJobTemplateException
    doThrow(new NoSuchJobTemplateException("mock error"))
        .when(jobOperationDispatcher)
        .runJob(any(), any(), any());

    Response resp2 =
        target(jobRunPath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchJobTemplateException.class.getSimpleName(), errorResp.getType());
  }

  @Test
  public void testCancelJob() {
    JobEntity job = newJobEntity("shell_template_1", JobHandle.Status.STARTED);

    when(jobOperationDispatcher.cancelJob(metalake, job.name())).thenReturn(job);

    Response resp =
        target(jobRunPath())
            .path(job.name())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(null);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(APPLICATION_JSON_TYPE, resp.getMediaType());

    JobResponse jobResp = resp.readEntity(JobResponse.class);
    Assertions.assertEquals(0, jobResp.getCode());
    Assertions.assertEquals(JobOperations.toDTO(job), jobResp.getJob());

    // Test throw NoSuchJobException
    doThrow(new NoSuchJobException("mock error"))
        .when(jobOperationDispatcher)
        .cancelJob(any(), any());

    Response resp2 =
        target(jobRunPath())
            .path(job.name())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(null);

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchJobException.class.getSimpleName(), errorResp.getType());
  }

  private String jobTemplatePath() {
    return "/metalakes/" + metalake + "/jobs/templates";
  }

  private String jobRunPath() {
    return "/metalakes/" + metalake + "/jobs/runs";
  }

  private JobTemplateEntity newShellJobTemplateEntity(String name, String comment) {
    ShellJobTemplate shellJobTemplate =
        ShellJobTemplate.builder()
            .withName(name)
            .withComment(comment)
            .withExecutable("/bin/echo")
            .build();

    Random rand = new Random();
    return JobTemplateEntity.builder()
        .withId(rand.nextLong())
        .withName(name)
        .withNamespace(NamespaceUtil.ofJobTemplate(metalake))
        .withTemplateContent(JobTemplateEntity.TemplateContent.fromJobTemplate(shellJobTemplate))
        .withAuditInfo(auditInfo)
        .build();
  }

  private JobTemplateEntity newSparkJobTemplateEntity(String name, String comment) {
    SparkJobTemplate sparkJobTemplate =
        SparkJobTemplate.builder()
            .withName(name)
            .withComment(comment)
            .withClassName("org.apache.spark.examples.SparkPi")
            .withExecutable("file:/path/to/spark-examples.jar")
            .build();

    Random rand = new Random();
    return JobTemplateEntity.builder()
        .withId(rand.nextLong())
        .withName(name)
        .withNamespace(NamespaceUtil.ofJobTemplate(metalake))
        .withTemplateContent(JobTemplateEntity.TemplateContent.fromJobTemplate(sparkJobTemplate))
        .withAuditInfo(auditInfo)
        .build();
  }

  private JobEntity newJobEntity(String templateName, JobHandle.Status status) {
    Random rand = new Random();
    return JobEntity.builder()
        .withId(rand.nextLong())
        .withJobExecutionId(rand.nextLong() + "")
        .withNamespace(NamespaceUtil.ofJob(metalake))
        .withJobTemplateName(templateName)
        .withStatus(status)
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
