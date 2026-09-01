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
import static org.apache.gravitino.Configs.CACHE_ENABLED;
import static org.apache.gravitino.Configs.ENABLE_AUTHORIZATION;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.dto.job.JobDTO;
import org.apache.gravitino.dto.job.JobTemplateDTO;
import org.apache.gravitino.dto.job.ShellJobTemplateDTO;
import org.apache.gravitino.dto.job.ShellTemplateUpdateDTO;
import org.apache.gravitino.dto.requests.JobRunRequest;
import org.apache.gravitino.dto.requests.JobTemplateRegisterRequest;
import org.apache.gravitino.dto.requests.JobTemplateUpdateRequest;
import org.apache.gravitino.dto.requests.JobTemplateUpdatesRequest;
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
import org.apache.gravitino.job.JobTemplateChange;
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
    Config config = mock(Config.class);
    doReturn(false).when(config).get(CACHE_ENABLED);
    doReturn(false).when(config).get(ENABLE_AUTHORIZATION);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);

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
  public void testAlterJobTemplateWithNullRequest() {
    Response resp =
        target(jobTemplatePath())
            .path("shell_template_1")
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(new byte[0], APPLICATION_JSON_TYPE));

    BaseOperationsTest.assertNullRequestBodyRejected(resp);
  }

  @Test
  public void testAlterJobTemplate() {
    String templateName = "shell_template_1";
    JobTemplateEntity template = newShellJobTemplateEntity(templateName, "Updated comment");
    JobTemplateUpdateRequest renameReq =
        new JobTemplateUpdateRequest.RenameJobTemplateRequest(templateName);
    JobTemplateUpdateRequest updateCommentReq =
        new JobTemplateUpdateRequest.UpdateJobTemplateCommentRequest("Updated comment");
    JobTemplateUpdateRequest updateContentReq =
        new JobTemplateUpdateRequest.UpdateJobTemplateContentRequest(
            ShellTemplateUpdateDTO.builder().build());
    JobTemplateUpdatesRequest req =
        new JobTemplateUpdatesRequest(
            Lists.newArrayList(renameReq, updateCommentReq, updateContentReq));
    JobTemplateChange[] changes =
        req.getUpdates().stream()
            .map(JobTemplateUpdateRequest::jobTemplateChange)
            .toArray(JobTemplateChange[]::new);

    when(jobOperationDispatcher.alterJobTemplate(metalake, templateName, changes))
        .thenReturn(template);

    Response resp =
        target(jobTemplatePath())
            .path(templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(APPLICATION_JSON_TYPE, resp.getMediaType());

    JobTemplateResponse jobTemplateResp = resp.readEntity(JobTemplateResponse.class);
    Assertions.assertEquals(0, jobTemplateResp.getCode());
    Assertions.assertEquals(JobOperations.toDTO(template), jobTemplateResp.getJobTemplate());

    // Test throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(jobOperationDispatcher)
        .alterJobTemplate(any(), any(), any());

    Response resp2 =
        target(jobTemplatePath())
            .path(templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResp.getType());

    // Test throw MetalakeNotInUseException
    doThrow(new MetalakeNotInUseException("mock error"))
        .when(jobOperationDispatcher)
        .alterJobTemplate(any(), any(), any());

    Response resp3 =
        target(jobTemplatePath())
            .path(templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_IN_USE_CODE, errorResp2.getCode());
    Assertions.assertEquals(MetalakeNotInUseException.class.getSimpleName(), errorResp2.getType());

    // Test throw IllegalArgumentException
    doThrow(new IllegalArgumentException("mock error"))
        .when(jobOperationDispatcher)
        .alterJobTemplate(any(), any(), any());

    Response resp4 =
        target(jobTemplatePath())
            .path(templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp4.getStatus());
    ErrorResponse errorResp3 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp3.getCode());
    Assertions.assertEquals(IllegalArgumentException.class.getSimpleName(), errorResp3.getType());

    // Test throw NoSuchJobTemplateException
    doThrow(new NoSuchJobTemplateException("mock error"))
        .when(jobOperationDispatcher)
        .alterJobTemplate(any(), any(), any());

    Response resp5 =
        target(jobTemplatePath())
            .path(templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp5.getStatus());
    ErrorResponse errorResp4 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp4.getCode());
    Assertions.assertEquals(NoSuchJobTemplateException.class.getSimpleName(), errorResp4.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(jobOperationDispatcher)
        .alterJobTemplate(any(), any(), any());

    Response resp6 =
        target(jobTemplatePath())
            .path(templateName)
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp6.getStatus());
    ErrorResponse errorResp5 = resp6.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp5.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp5.getType());
  }

  @Test
  public void testListJobs() {
    String templateName = "shell_template_1";
    // Fixed, strictly-increasing queuedAt values rather than back-to-back Instant.now() calls -
    // the latter can collide (millisecond clock resolution on some JDKs/OSes), which would make
    // the desc-sort assertions below flaky.
    JobEntity job1 = newJobEntityWithQueuedAt(templateName, JobHandle.Status.QUEUED, 1000L, 0L, 0L);
    JobEntity job2 =
        newJobEntityWithQueuedAt(templateName, JobHandle.Status.STARTED, 2000L, 2500L, 0L);
    JobEntity job3 =
        newJobEntityWithQueuedAt("spark_template_1", JobHandle.Status.SUCCEEDED, 3000L, 0L, 3500L);

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

    // statusCounts reflects the returned jobs: one QUEUED, one STARTED, one SUCCEEDED, and every
    // other status present at zero.
    Map<String, Long> expectedStatusCounts = new HashMap<>();
    expectedStatusCounts.put("queued", 1L);
    expectedStatusCounts.put("started", 1L);
    expectedStatusCounts.put("failed", 0L);
    expectedStatusCounts.put("succeeded", 1L);
    expectedStatusCounts.put("cancelling", 0L);
    expectedStatusCounts.put("cancelled", 0L);
    Assertions.assertEquals(expectedStatusCounts, jobListResponse.getStatusCounts());

    // Default sort is queuedAt desc (newest first); job1/job2/job3 have strictly increasing
    // queuedAt (1000L < 2000L < 3000L), so the response reverses that order.
    Assertions.assertEquals(3, jobListResponse.getJobs().size());
    Assertions.assertEquals(JobOperations.toDTO(job3), jobListResponse.getJobs().get(0));
    Assertions.assertEquals(JobOperations.toDTO(job2), jobListResponse.getJobs().get(1));
    Assertions.assertEquals(JobOperations.toDTO(job1), jobListResponse.getJobs().get(2));

    // A finished job round-trips its finishedAt as an Instant over the wire.
    Assertions.assertEquals(
        Instant.ofEpochMilli(job3.finishedAt()), jobListResponse.getJobs().get(0).finishedAt());
    // Not-yet-finished jobs round-trip finishedAt as null over the wire.
    Assertions.assertNull(jobListResponse.getJobs().get(1).finishedAt());
    Assertions.assertNull(jobListResponse.getJobs().get(2).finishedAt());

    // queuedAt is always present, regardless of status.
    Assertions.assertNotNull(jobListResponse.getJobs().get(0).queuedAt());
    Assertions.assertNotNull(jobListResponse.getJobs().get(1).queuedAt());
    Assertions.assertNotNull(jobListResponse.getJobs().get(2).queuedAt());

    // A started job round-trips its startedAt as an Instant over the wire.
    Assertions.assertEquals(
        Instant.ofEpochMilli(job2.startedAt()), jobListResponse.getJobs().get(1).startedAt());
    // A not-yet-started job round-trips startedAt as null over the wire.
    Assertions.assertNull(jobListResponse.getJobs().get(2).startedAt());

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
    // Default sort is queuedAt desc: job2 (queued after job1) comes first.
    Assertions.assertEquals(JobOperations.toDTO(job2), jobListResponse1.getJobs().get(0));
    Assertions.assertEquals(JobOperations.toDTO(job1), jobListResponse1.getJobs().get(1));

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
  public void testListJobsWithTimeFiltersAndSort() {
    String templateName = "shell_template_1";
    // job1: queued only. job2: queued+started. job3: queued+started+finished, queued earliest.
    JobEntity job1 = newJobEntityWithQueuedAt(templateName, JobHandle.Status.QUEUED, 3000L, 0L, 0L);
    JobEntity job2 =
        newJobEntityWithQueuedAt(templateName, JobHandle.Status.STARTED, 2000L, 2500L, 0L);
    JobEntity job3 =
        newJobEntityWithQueuedAt(templateName, JobHandle.Status.SUCCEEDED, 1000L, 1500L, 1800L);

    when(jobOperationDispatcher.listJobs(metalake, Optional.empty()))
        .thenReturn(Lists.newArrayList(job1, job2, job3));

    // startedAfter excludes job1 (never started); default sort is queuedAt desc.
    Response resp =
        target(jobRunPath())
            .queryParam("startedAfter", Instant.ofEpochMilli(1000L).toString())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    JobListResponse jobListResponse = resp.readEntity(JobListResponse.class);
    Assertions.assertEquals(2, jobListResponse.getJobs().size());
    Assertions.assertEquals(JobOperations.toDTO(job2), jobListResponse.getJobs().get(0));
    Assertions.assertEquals(JobOperations.toDTO(job3), jobListResponse.getJobs().get(1));

    // statusCounts is scoped to the filtered set: job1 (QUEUED) is excluded by startedAfter,
    // so its status contributes zero here, even though the job itself exists.
    Map<String, Long> expectedFilteredStatusCounts = new HashMap<>();
    expectedFilteredStatusCounts.put("queued", 0L);
    expectedFilteredStatusCounts.put("started", 1L);
    expectedFilteredStatusCounts.put("failed", 0L);
    expectedFilteredStatusCounts.put("succeeded", 1L);
    expectedFilteredStatusCounts.put("cancelling", 0L);
    expectedFilteredStatusCounts.put("cancelled", 0L);
    Assertions.assertEquals(expectedFilteredStatusCounts, jobListResponse.getStatusCounts());

    // sortBy=startedAt, sortOrder=asc: job3 (1500) < job2 (2500) < job1 (null, sorts last).
    Response resp2 =
        target(jobRunPath())
            .queryParam("sortBy", "startedAt")
            .queryParam("sortOrder", "asc")
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    JobListResponse jobListResponse2 = resp2.readEntity(JobListResponse.class);
    Assertions.assertEquals(3, jobListResponse2.getJobs().size());
    Assertions.assertEquals(JobOperations.toDTO(job3), jobListResponse2.getJobs().get(0));
    Assertions.assertEquals(JobOperations.toDTO(job2), jobListResponse2.getJobs().get(1));
    Assertions.assertEquals(JobOperations.toDTO(job1), jobListResponse2.getJobs().get(2));

    // Invalid sortBy is rejected with 400.
    Response resp3 =
        target(jobRunPath())
            .queryParam("sortBy", "bogus")
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp3.getStatus());
    ErrorResponse errorResp3 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp3.getCode());
    Assertions.assertEquals(IllegalArgumentException.class.getSimpleName(), errorResp3.getType());

    // Invalid sortOrder is rejected with 400.
    Response resp4 =
        target(jobRunPath())
            .queryParam("sortOrder", "sideways")
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp4.getStatus());
    ErrorResponse errorResp4 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp4.getCode());
    Assertions.assertEquals(IllegalArgumentException.class.getSimpleName(), errorResp4.getType());

    // Non-ISO-8601 time value is rejected with 400.
    Response resp5 =
        target(jobRunPath())
            .queryParam("queuedAfter", "not-a-timestamp")
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp5.getStatus());
    ErrorResponse errorResp5 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp5.getCode());
    Assertions.assertEquals(IllegalArgumentException.class.getSimpleName(), errorResp5.getType());

    // Blank query params (e.g. "?sortBy=&sortOrder=&queuedAfter="), as templated/generated
    // clients sometimes send, fall back to "not set" rather than 400ing: @DefaultValue only
    // applies when a param is absent, not when it's present-but-empty.
    Response resp6 =
        target(jobRunPath())
            .queryParam("sortBy", "")
            .queryParam("sortOrder", "")
            .queryParam("queuedAfter", "")
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp6.getStatus());
    JobListResponse jobListResponse6 = resp6.readEntity(JobListResponse.class);
    Assertions.assertEquals(3, jobListResponse6.getJobs().size());
    Assertions.assertEquals(JobOperations.toDTO(job1), jobListResponse6.getJobs().get(0));
    Assertions.assertEquals(JobOperations.toDTO(job2), jobListResponse6.getJobs().get(1));
    Assertions.assertEquals(JobOperations.toDTO(job3), jobListResponse6.getJobs().get(2));
  }

  @Test
  public void testListJobsCombinesTemplateFilterWithTimeFiltersAndSort() {
    String templateName = "shell_template_1";
    JobEntity job1 = newJobEntityWithQueuedAt(templateName, JobHandle.Status.QUEUED, 3000L, 0L, 0L);
    JobEntity job2 =
        newJobEntityWithQueuedAt(templateName, JobHandle.Status.STARTED, 2000L, 2500L, 0L);
    JobEntity job3 =
        newJobEntityWithQueuedAt(templateName, JobHandle.Status.SUCCEEDED, 1000L, 1500L, 1800L);

    // The dispatcher already narrows to this template's jobs; jobs from other templates (e.g. a
    // spark_template_1 job) are never in this list, so they can't leak in via the time filter.
    when(jobOperationDispatcher.listJobs(metalake, Optional.of(templateName)))
        .thenReturn(Lists.newArrayList(job1, job2, job3));

    Response resp =
        target(jobRunPath())
            .queryParam("jobTemplateName", templateName)
            .queryParam("startedAfter", Instant.ofEpochMilli(1000L).toString())
            .queryParam("sortBy", "startedAt")
            .queryParam("sortOrder", "asc")
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    JobListResponse jobListResponse = resp.readEntity(JobListResponse.class);
    // job1 is excluded (never started); job3 (startedAt=1500) sorts before job2 (startedAt=2500).
    Assertions.assertEquals(2, jobListResponse.getJobs().size());
    Assertions.assertEquals(JobOperations.toDTO(job3), jobListResponse.getJobs().get(0));
    Assertions.assertEquals(JobOperations.toDTO(job2), jobListResponse.getJobs().get(1));
  }

  @Test
  public void testFilterAndSortJobs() {
    String templateName = "shell_template_1";
    JobEntity job1 = newJobEntityWithQueuedAt(templateName, JobHandle.Status.QUEUED, 1000L, 0L, 0L);
    JobEntity job2 =
        newJobEntityWithQueuedAt(templateName, JobHandle.Status.STARTED, 2000L, 3000L, 0L);
    JobEntity job3 =
        newJobEntityWithQueuedAt(templateName, JobHandle.Status.SUCCEEDED, 3000L, 4000L, 5000L);
    List<JobEntity> jobs = Lists.newArrayList(job1, job2, job3);
    Comparator<JobEntity> queuedAtAsc = JobOperations.buildJobComparator("queuedAt", "asc");

    // queuedAfter filters out job1.
    List<JobEntity> filteredByQueuedAfter =
        JobOperations.filterAndSortJobs(jobs, instant(1500L), null, null, queuedAtAsc);
    Assertions.assertEquals(Lists.newArrayList(job2, job3), filteredByQueuedAfter);

    // startedAfter excludes not-yet-started job1 and jobs started before the bound.
    List<JobEntity> filteredByStartedAfter =
        JobOperations.filterAndSortJobs(jobs, null, instant(3500L), null, queuedAtAsc);
    Assertions.assertEquals(Lists.newArrayList(job3), filteredByStartedAfter);

    // finishedAfter excludes not-yet-finished jobs.
    List<JobEntity> filteredByFinishedAfter =
        JobOperations.filterAndSortJobs(jobs, null, null, instant(1L), queuedAtAsc);
    Assertions.assertEquals(Lists.newArrayList(job3), filteredByFinishedAfter);

    // Filters are AND-combined.
    List<JobEntity> filteredByAll =
        JobOperations.filterAndSortJobs(
            jobs, instant(1500L), instant(3500L), instant(1L), queuedAtAsc);
    Assertions.assertEquals(Lists.newArrayList(job3), filteredByAll);

    // Boundary is inclusive (>=).
    List<JobEntity> filteredInclusive =
        JobOperations.filterAndSortJobs(jobs, instant(2000L), null, null, queuedAtAsc);
    Assertions.assertEquals(Lists.newArrayList(job2, job3), filteredInclusive);

    // sortBy=queuedAt desc.
    List<JobEntity> sortedByQueuedDesc =
        JobOperations.filterAndSortJobs(
            jobs, null, null, null, JobOperations.buildJobComparator("queuedAt", "desc"));
    Assertions.assertEquals(Lists.newArrayList(job3, job2, job1), sortedByQueuedDesc);

    // sortBy=startedAt asc, jobs without startedAt sort last regardless of direction.
    List<JobEntity> sortedByStartedAsc =
        JobOperations.filterAndSortJobs(
            jobs, null, null, null, JobOperations.buildJobComparator("startedAt", "asc"));
    Assertions.assertEquals(Lists.newArrayList(job2, job3, job1), sortedByStartedAsc);

    List<JobEntity> sortedByStartedDesc =
        JobOperations.filterAndSortJobs(
            jobs, null, null, null, JobOperations.buildJobComparator("startedAt", "desc"));
    Assertions.assertEquals(Lists.newArrayList(job3, job2, job1), sortedByStartedDesc);
  }

  @Test
  public void testCountJobsByStatus() {
    String templateName = "shell_template_1";
    JobEntity queuedJob1 = newJobEntity(templateName, JobHandle.Status.QUEUED);
    JobEntity queuedJob2 = newJobEntity(templateName, JobHandle.Status.QUEUED);
    JobEntity startedJob =
        newJobEntity(templateName, JobHandle.Status.STARTED, Instant.now().toEpochMilli(), 0L);
    JobEntity succeededJob =
        newJobEntity(templateName, JobHandle.Status.SUCCEEDED, Instant.now().toEpochMilli());

    Map<String, Long> counts =
        JobOperations.countJobsByStatus(
            Lists.newArrayList(queuedJob1, queuedJob2, startedJob, succeededJob));

    // Every JobHandle.Status is present, even at zero.
    Map<String, Long> expected = new HashMap<>();
    expected.put("queued", 2L);
    expected.put("started", 1L);
    expected.put("failed", 0L);
    expected.put("succeeded", 1L);
    expected.put("cancelling", 0L);
    expected.put("cancelled", 0L);
    Assertions.assertEquals(expected, counts);

    // An empty job list still reports every status at zero.
    Map<String, Long> emptyCounts = JobOperations.countJobsByStatus(Lists.newArrayList());
    Assertions.assertEquals(6, emptyCounts.size());
    emptyCounts.values().forEach(count -> Assertions.assertEquals(0L, count));
  }

  @Test
  public void testParseInstant() {
    Assertions.assertNull(JobOperations.parseInstant("queuedAfter", null));
    Assertions.assertNull(JobOperations.parseInstant("queuedAfter", ""));

    Instant expected = Instant.ofEpochMilli(1500L);
    Assertions.assertEquals(
        expected, JobOperations.parseInstant("queuedAfter", expected.toString()));

    // RFC 3339 numeric offsets (as advertised by the OpenAPI `format: date-time`) are accepted,
    // not just the strict ISO_INSTANT `Z` form.
    Assertions.assertEquals(
        Instant.parse("2026-08-17T16:00:00Z"),
        JobOperations.parseInstant("queuedAfter", "2026-08-18T00:00:00+08:00"));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> JobOperations.parseInstant("queuedAfter", "not-a-timestamp"));
  }

  @Test
  public void testBuildJobComparatorValidation() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> JobOperations.buildJobComparator("bogus", "asc"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> JobOperations.buildJobComparator("queuedAt", "bogus"));

    // sortBy/sortOrder are matched exactly against the OpenAPI-documented casing, not
    // case-insensitively.
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> JobOperations.buildJobComparator("queuedat", "asc"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> JobOperations.buildJobComparator("queuedAt", "ASC"));
  }

  private static Instant instant(long epochMilli) {
    return Instant.ofEpochMilli(epochMilli);
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
    long startedAt = Instant.now().toEpochMilli() - 1000;
    long finishedAt = Instant.now().toEpochMilli();
    JobEntity job =
        newJobEntity("shell_template_1", JobHandle.Status.CANCELLED, startedAt, finishedAt);

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
    // queuedAt is always present, regardless of status.
    Assertions.assertNotNull(jobResp.getJob().queuedAt());
    // A cancelled job that had started round-trips its startedAt as an Instant over the wire.
    Assertions.assertEquals(Instant.ofEpochMilli(job.startedAt()), jobResp.getJob().startedAt());
    // A finished (cancelled) job round-trips its finishedAt as an Instant over the wire.
    Assertions.assertEquals(Instant.ofEpochMilli(job.finishedAt()), jobResp.getJob().finishedAt());

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

  @Test
  public void testCancelJobWithMalformedRuntimeJobTemplateDoesNotFail() {
    // By the time toDTO() runs here, jobOperationDispatcher.cancelJob() has already cancelled
    // the job and updated its stored entity - a malformed stored runtime job template must not
    // turn that already-completed cancellation into a 500 for the caller. The response should
    // just omit the runtime job template.
    JobEntity job =
        JobEntity.builder()
            .withId(new Random().nextLong())
            .withJobExecutionId("job-execution-cancel-malformed")
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withJobTemplateName("shell_template_1")
            .withStatus(JobHandle.Status.CANCELLED)
            .withStartedAt(0L)
            .withFinishedAt(Instant.now().toEpochMilli())
            .withRuntimeJobTemplate("{not-valid-json")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    when(jobOperationDispatcher.cancelJob(metalake, job.name())).thenReturn(job);

    Response resp =
        target(jobRunPath())
            .path(job.name())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(null);

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    JobResponse jobResp = resp.readEntity(JobResponse.class);
    Assertions.assertEquals(0, jobResp.getCode());
    Assertions.assertEquals(JobHandle.Status.CANCELLED, jobResp.getJob().status());
    Assertions.assertNull(jobResp.getJob().runtimeJobTemplate());
  }

  @Test
  public void testToDTOFinishedAt() {
    // Sentinel value (<= 0) used by the storage layer means "not finished".
    JobEntity sentinelJob = newJobEntity("shell_template_1", JobHandle.Status.STARTED, 0L);
    JobDTO sentinelJobDTO = JobOperations.toDTO(sentinelJob);
    Assertions.assertNull(sentinelJobDTO.finishedAt());

    // Finished, finishedAt is converted from epoch millis to an Instant.
    long epochMilli = Instant.now().toEpochMilli();
    JobEntity finishedJob =
        newJobEntity("shell_template_1", JobHandle.Status.SUCCEEDED, epochMilli);
    JobDTO finishedJobDTO = JobOperations.toDTO(finishedJob);
    Assertions.assertEquals(Instant.ofEpochMilli(epochMilli), finishedJobDTO.finishedAt());
  }

  @Test
  public void testToDTOStartedAt() {
    // Sentinel value (<= 0) used by the storage layer means "not started".
    JobEntity sentinelJob = newJobEntity("shell_template_1", JobHandle.Status.QUEUED, 0L, 0L);
    JobDTO sentinelJobDTO = JobOperations.toDTO(sentinelJob);
    Assertions.assertNull(sentinelJobDTO.startedAt());

    // Started, startedAt is converted from epoch millis to an Instant.
    long epochMilli = Instant.now().toEpochMilli();
    JobEntity startedJob =
        newJobEntity("shell_template_1", JobHandle.Status.STARTED, epochMilli, 0L);
    JobDTO startedJobDTO = JobOperations.toDTO(startedJob);
    Assertions.assertEquals(Instant.ofEpochMilli(epochMilli), startedJobDTO.startedAt());
  }

  @Test
  public void testToDTOQueuedAt() {
    // queuedAt is always present - it's the job's creation time, not a sentinel-backed field.
    JobEntity job = newJobEntity("shell_template_1", JobHandle.Status.QUEUED);
    JobDTO jobDTO = JobOperations.toDTO(job);
    Assertions.assertEquals(job.auditInfo().createTime(), jobDTO.queuedAt());
    Assertions.assertNotNull(jobDTO.queuedAt());
  }

  @Test
  public void testToDTORuntimeJobTemplate() {
    // No runtime job template stored (e.g. a job run before this field was introduced) - must
    // round-trip as null rather than failing to convert.
    JobEntity jobWithoutTemplate = newJobEntity("shell_template_1", JobHandle.Status.QUEUED);
    JobDTO jobDTOWithoutTemplate = JobOperations.toDTO(jobWithoutTemplate);
    Assertions.assertNull(jobDTOWithoutTemplate.runtimeJobTemplate());

    // A stored runtime job template must be deserialized back into a JobTemplateDTO, with
    // Shell/Spark dispatch handled automatically by JobTemplateDTO's @JsonTypeInfo.
    String runtimeJobTemplateJson =
        "{\"jobType\":\"shell\",\"name\":\"shell_template_1\",\"comment\":\"resolved\","
            + "\"executable\":\"/bin/echo\",\"arguments\":[\"resolved-arg\"]}";
    JobEntity jobWithTemplate =
        JobEntity.builder()
            .withId(new Random().nextLong())
            .withJobExecutionId("job-execution-with-template")
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withJobTemplateName("shell_template_1")
            .withStatus(JobHandle.Status.QUEUED)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .withRuntimeJobTemplate(runtimeJobTemplateJson)
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    JobDTO jobDTO = JobOperations.toDTO(jobWithTemplate);

    Assertions.assertNotNull(jobDTO.runtimeJobTemplate());
    Assertions.assertInstanceOf(ShellJobTemplateDTO.class, jobDTO.runtimeJobTemplate());
    ShellJobTemplateDTO runtimeJobTemplateDTO = (ShellJobTemplateDTO) jobDTO.runtimeJobTemplate();
    Assertions.assertEquals("shell_template_1", runtimeJobTemplateDTO.name());
    Assertions.assertEquals("resolved", runtimeJobTemplateDTO.comment());
    Assertions.assertEquals("/bin/echo", runtimeJobTemplateDTO.executable());
    Assertions.assertEquals(Lists.newArrayList("resolved-arg"), runtimeJobTemplateDTO.arguments());
  }

  @Test
  public void testListJobsWithMalformedRuntimeJobTemplateDoesNotFailWholeList() {
    // A single job whose stored runtime job template fails to deserialize (e.g. corrupted or
    // written by a future, incompatible version) must not fail the entire listJobs response -
    // it should come back with a null runtimeJobTemplate while every other job is unaffected.
    String templateName = "shell_template_1";
    JobEntity healthyJob = newJobEntity(templateName, JobHandle.Status.QUEUED);
    JobEntity malformedJob =
        JobEntity.builder()
            .withId(new Random().nextLong())
            .withJobExecutionId("job-execution-malformed")
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withJobTemplateName(templateName)
            .withStatus(JobHandle.Status.QUEUED)
            .withStartedAt(0L)
            .withFinishedAt(0L)
            .withRuntimeJobTemplate("{not-valid-json")
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();

    when(jobOperationDispatcher.listJobs(metalake, Optional.empty()))
        .thenReturn(Lists.newArrayList(healthyJob, malformedJob));

    Response resp =
        target(jobRunPath())
            .request(APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    JobListResponse jobListResponse = resp.readEntity(JobListResponse.class);
    Assertions.assertEquals(0, jobListResponse.getCode());
    Assertions.assertEquals(2, jobListResponse.getJobs().size());

    JobDTO healthyJobDTO =
        jobListResponse.getJobs().stream()
            .filter(dto -> dto.jobId().equals(healthyJob.name()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Healthy job missing from response"));
    Assertions.assertEquals(JobOperations.toDTO(healthyJob), healthyJobDTO);

    JobDTO malformedJobDTO =
        jobListResponse.getJobs().stream()
            .filter(dto -> dto.jobId().equals(malformedJob.name()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Malformed job missing from response"));
    Assertions.assertNull(malformedJobDTO.runtimeJobTemplate());
    Assertions.assertEquals(JobHandle.Status.QUEUED, malformedJobDTO.status());
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
    return newJobEntity(templateName, status, 0L, 0L);
  }

  private JobEntity newJobEntity(String templateName, JobHandle.Status status, Long finishedAt) {
    return newJobEntity(templateName, status, 0L, finishedAt);
  }

  private JobEntity newJobEntity(
      String templateName, JobHandle.Status status, Long startedAt, Long finishedAt) {
    Random rand = new Random();
    return JobEntity.builder()
        .withId(rand.nextLong())
        .withJobExecutionId(rand.nextLong() + "")
        .withNamespace(NamespaceUtil.ofJob(metalake))
        .withJobTemplateName(templateName)
        .withStatus(status)
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .withStartedAt(startedAt)
        .withFinishedAt(finishedAt)
        .build();
  }

  private JobEntity newJobEntityWithQueuedAt(
      String templateName,
      JobHandle.Status status,
      long queuedAtEpochMilli,
      Long startedAt,
      Long finishedAt) {
    Random rand = new Random();
    return JobEntity.builder()
        .withId(rand.nextLong())
        .withJobExecutionId(rand.nextLong() + "")
        .withNamespace(NamespaceUtil.ofJob(metalake))
        .withJobTemplateName(templateName)
        .withStatus(status)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator("test")
                .withCreateTime(Instant.ofEpochMilli(queuedAtEpochMilli))
                .build())
        .withStartedAt(startedAt)
        .withFinishedAt(finishedAt)
        .build();
  }
}
