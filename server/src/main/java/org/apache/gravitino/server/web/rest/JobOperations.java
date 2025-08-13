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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.dto.job.JobDTO;
import org.apache.gravitino.dto.job.JobTemplateDTO;
import org.apache.gravitino.dto.job.ShellJobTemplateDTO;
import org.apache.gravitino.dto.job.SparkJobTemplateDTO;
import org.apache.gravitino.dto.requests.JobRunRequest;
import org.apache.gravitino.dto.requests.JobTemplateRegisterRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.JobListResponse;
import org.apache.gravitino.dto.responses.JobResponse;
import org.apache.gravitino.dto.responses.JobTemplateListResponse;
import org.apache.gravitino.dto.responses.JobTemplateResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("metalakes/{metalake}/jobs")
public class JobOperations {

  private static final Logger LOG = LoggerFactory.getLogger(JobOperations.class);

  private final JobOperationDispatcher jobOperationDispatcher;

  @Context HttpServletRequest httpRequest;

  @Inject
  public JobOperations(JobOperationDispatcher jobOperationDispatcher) {
    this.jobOperationDispatcher = jobOperationDispatcher;
  }

  @GET
  @Path("templates")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-job-templates." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-job-templates", absolute = true)
  public Response listJobTemplates(
      @PathParam("metalake") String metalake,
      @QueryParam("details") @DefaultValue("false") boolean details) {
    LOG.info(
        "Received request to list job templates in metalake: {}, details: {}", metalake, details);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            List<JobTemplateDTO> jobTemplates =
                toJobTemplateDTOs(jobOperationDispatcher.listJobTemplates(metalake));
            if (details) {
              LOG.info("List {} job templates in metalake: {}", jobTemplates.size(), metalake);
              return Utils.ok(new JobTemplateListResponse(jobTemplates));

            } else {
              String[] jobTemplateNames =
                  jobTemplates.stream().map(JobTemplateDTO::name).toArray(String[]::new);

              LOG.info(
                  "List {} job template names in metalake: {}", jobTemplateNames.length, metalake);
              return Utils.ok(new NameListResponse(jobTemplateNames));
            }
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleJobTemplateException(OperationType.LIST, "", metalake, e);
    }
  }

  @POST
  @Path("templates")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "register-job-template." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "register-job-template", absolute = true)
  public Response registerJobTemplate(
      @PathParam("metalake") String metalake, JobTemplateRegisterRequest request) {
    LOG.info(
        "Received request to register job template {} in metalake: {}",
        request.getJobTemplate().name(),
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();

            jobOperationDispatcher.registerJobTemplate(
                metalake, toEntity(metalake, request.getJobTemplate()));

            LOG.info(
                "Registered job template {} in metalake: {}",
                request.getJobTemplate().name(),
                metalake);
            return Utils.ok(new BaseResponse());
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleJobTemplateException(
          OperationType.REGISTER, request.getJobTemplate().name(), metalake, e);
    }
  }

  @GET
  @Path("templates/{name}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-job-template." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-job-template", absolute = true)
  public Response getJobTemplate(
      @PathParam("metalake") String metalake, @PathParam("name") String name) {
    LOG.info("Received request to get job template: {} in metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            JobTemplateEntity jobTemplateEntity =
                jobOperationDispatcher.getJobTemplate(metalake, name);

            LOG.info("Retrieved job template {} in metalake: {}", name, metalake);
            return Utils.ok(new JobTemplateResponse(toDTO(jobTemplateEntity)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleJobTemplateException(OperationType.GET, name, metalake, e);
    }
  }

  @DELETE
  @Path("templates/{name}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "delete-job-template." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-job-template", absolute = true)
  public Response deleteJobTemplate(
      @PathParam("metalake") String metalake, @PathParam("name") String name) {
    LOG.info("Received request to delete job template: {} in metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deleted = jobOperationDispatcher.deleteJobTemplate(metalake, name);
            if (!deleted) {
              LOG.warn("Cannot find job template {} in metalake {}", name, metalake);
            } else {
              LOG.info("Deleted job template {} in metalake {}", name, metalake);
            }

            return Utils.ok(new DropResponse(deleted));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleJobTemplateException(OperationType.DELETE, name, metalake, e);
    }
  }

  @GET
  @Path("runs")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-jobs." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-jobs", absolute = true)
  public Response listJobs(
      @PathParam("metalake") String metalake,
      @QueryParam("jobTemplateName") String jobTemplateName) {
    LOG.info(
        "Received request to list jobs in metalake {}{}",
        metalake,
        jobTemplateName != null ? " for job template " + jobTemplateName : "");

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            List<JobEntity> jobEntities =
                jobOperationDispatcher.listJobs(metalake, Optional.ofNullable(jobTemplateName));
            List<JobDTO> jobDTOs = toJobDTOs(jobEntities);

            LOG.info("Listed {} jobs in metalake {}", jobEntities.size(), metalake);
            return Utils.ok(new JobListResponse(jobDTOs));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleJobException(OperationType.LIST, "", metalake, e);
    }
  }

  @GET
  @Path("runs/{jobId}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-job." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-job", absolute = true)
  public Response getJob(@PathParam("metalake") String metalake, @PathParam("jobId") String jobId) {
    LOG.info("Received request to get job {} in metalake {}", jobId, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            JobEntity jobEntity = jobOperationDispatcher.getJob(metalake, jobId);
            LOG.info("Retrieved job {} in metalake: {}", jobId, metalake);
            return Utils.ok(new JobResponse(toDTO(jobEntity)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleJobException(OperationType.GET, jobId, metalake, e);
    }
  }

  @POST
  @Path("runs")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "run-job." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "run-job", absolute = true)
  public Response runJob(@PathParam("metalake") String metalake, JobRunRequest request) {
    LOG.info(
        "Received request to run job {} in metalake: {}", request.getJobTemplateName(), metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            Map<String, String> jobConf =
                request.getJobConf() != null ? request.getJobConf() : Collections.emptyMap();

            JobEntity jobEntity =
                jobOperationDispatcher.runJob(metalake, request.getJobTemplateName(), jobConf);

            LOG.info(
                "Run job[{}] {} in metalake: {}",
                jobEntity.jobTemplateName(),
                jobEntity.name(),
                metalake);
            return Utils.ok(new JobResponse(toDTO(jobEntity)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleJobException(OperationType.RUN, "", metalake, e);
    }
  }

  @POST
  @Path("runs/{jobId}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "cancel-job." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  public Response cancelJob(
      @PathParam("metalake") String metalake, @PathParam("jobId") String jobId) {
    LOG.info("Received request to cancel job {} in metalake {}", jobId, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            JobEntity jobEntity = jobOperationDispatcher.cancelJob(metalake, jobId);

            LOG.info("Cancelled job {} in metalake: {}", jobId, metalake);
            return Utils.ok(new JobResponse(toDTO(jobEntity)));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleJobException(OperationType.CANCEL, jobId, metalake, e);
    }
  }

  private static List<JobTemplateDTO> toJobTemplateDTOs(
      List<JobTemplateEntity> jobTemplateEntities) {
    return jobTemplateEntities.stream().map(JobOperations::toDTO).collect(Collectors.toList());
  }

  @VisibleForTesting
  static JobTemplateDTO toDTO(JobTemplateEntity jobTemplateEntity) {
    switch (jobTemplateEntity.templateContent().jobType()) {
      case SHELL:
        return ShellJobTemplateDTO.builder()
            .withName(jobTemplateEntity.name())
            .withComment(jobTemplateEntity.comment())
            .withJobType(jobTemplateEntity.templateContent().jobType())
            .withExecutable(jobTemplateEntity.templateContent().executable())
            .withArguments(jobTemplateEntity.templateContent().arguments())
            .withEnvironments(jobTemplateEntity.templateContent().environments())
            .withCustomFields(jobTemplateEntity.templateContent().customFields())
            .withScripts(jobTemplateEntity.templateContent().scripts())
            .withAudit(DTOConverters.toDTO(jobTemplateEntity.auditInfo()))
            .build();

      case SPARK:
        return SparkJobTemplateDTO.builder()
            .withName(jobTemplateEntity.name())
            .withComment(jobTemplateEntity.comment())
            .withJobType(jobTemplateEntity.templateContent().jobType())
            .withExecutable(jobTemplateEntity.templateContent().executable())
            .withArguments(jobTemplateEntity.templateContent().arguments())
            .withEnvironments(jobTemplateEntity.templateContent().environments())
            .withCustomFields(jobTemplateEntity.templateContent().customFields())
            .withClassName(jobTemplateEntity.templateContent().className())
            .withJars(jobTemplateEntity.templateContent().jars())
            .withFiles(jobTemplateEntity.templateContent().files())
            .withArchives(jobTemplateEntity.templateContent().archives())
            .withConfigs(jobTemplateEntity.templateContent().configs())
            .withAudit(DTOConverters.toDTO(jobTemplateEntity.auditInfo()))
            .build();

      default:
        throw new IllegalArgumentException(
            "Unsupported job template type: " + jobTemplateEntity.templateContent().jobType());
    }
  }

  private static JobTemplateEntity toEntity(String metalake, JobTemplateDTO jobTemplateDTO) {
    return JobTemplateEntity.builder()
        .withId(GravitinoEnv.getInstance().idGenerator().nextId())
        .withName(jobTemplateDTO.name())
        .withNamespace(NamespaceUtil.ofJobTemplate(metalake))
        .withComment(jobTemplateDTO.comment())
        .withTemplateContent(
            JobTemplateEntity.TemplateContent.fromJobTemplate(
                DTOConverters.fromDTO(jobTemplateDTO)))
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                .withCreateTime(Instant.now())
                .build())
        .build();
  }

  @VisibleForTesting
  static JobDTO toDTO(JobEntity jobEntity) {
    return new JobDTO(
        jobEntity.name(),
        jobEntity.jobTemplateName(),
        jobEntity.status(),
        DTOConverters.toDTO(jobEntity.auditInfo()));
  }

  private static List<JobDTO> toJobDTOs(List<JobEntity> jobEntities) {
    return jobEntities.stream().map(JobOperations::toDTO).collect(Collectors.toList());
  }
}
