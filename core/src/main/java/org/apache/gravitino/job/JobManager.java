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

import static org.apache.gravitino.metalake.MetalakeManager.checkMetalake;

import com.google.common.annotations.VisibleForTesting;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.connector.job.JobExecutor;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;

public class JobManager implements JobOperationDispatcher, Closeable {

  private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{\\{([\\w.-]+)\\}\\}");

  private static final int TIMEOUT_IN_MS = 30 * 1000; // 30 seconds

  private final EntityStore entityStore;

  private final File stagingDir;

  private final JobExecutor jobExecutor;

  private final IdGenerator idGenerator;

  public JobManager(Config config, EntityStore entityStore, IdGenerator idGenerator) {
    this(config, entityStore, idGenerator, JobExecutorFactory.create(config));
  }

  @VisibleForTesting
  JobManager(
      Config config, EntityStore entityStore, IdGenerator idGenerator, JobExecutor jobExecutor) {
    this.entityStore = entityStore;
    this.jobExecutor = jobExecutor;
    this.idGenerator = idGenerator;

    String stagingDirPath = config.get(Configs.JOB_STAGING_DIR);
    this.stagingDir = new File(stagingDirPath);
    if (stagingDir.exists()) {
      if (!stagingDir.isDirectory()) {
        throw new IllegalArgumentException(
            String.format("Staging directory %s exists but is not a directory", stagingDirPath));
      }

      if (!(stagingDir.canExecute() && stagingDir.canRead() && stagingDir.canWrite())) {
        throw new IllegalArgumentException(
            String.format("Staging directory %s is not accessible", stagingDirPath));
      }
    } else {
      if (!stagingDir.mkdirs()) {
        throw new IllegalArgumentException(
            String.format("Failed to create staging directory %s", stagingDirPath));
      }
    }
  }

  @Override
  public List<JobTemplateEntity> listJobTemplates(String metalake) {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    Namespace jobTemplateNs = NamespaceUtil.ofJobTemplate(metalake);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(jobTemplateNs.levels()),
        LockType.READ,
        () -> {
          try {
            return entityStore.list(
                jobTemplateNs, JobTemplateEntity.class, Entity.EntityType.JOB_TEMPLATE);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public void registerJobTemplate(String metalake, JobTemplateEntity jobTemplateEntity)
      throws JobTemplateAlreadyExistsException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    NameIdentifier jobTemplateIdent =
        NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateEntity.name());
    TreeLockUtils.doWithTreeLock(
        jobTemplateIdent,
        LockType.WRITE,
        () -> {
          try {
            entityStore.put(jobTemplateEntity, false /* overwrite */);
            return null;
          } catch (EntityAlreadyExistsException e) {
            throw new JobTemplateAlreadyExistsException(
                "Job template with name %s under metalake %s already exists",
                jobTemplateEntity.name(), metalake);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public JobTemplateEntity getJobTemplate(String metalake, String jobTemplateName)
      throws NoSuchJobTemplateException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    NameIdentifier jobTemplateIdent = NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName);
    return TreeLockUtils.doWithTreeLock(
        jobTemplateIdent,
        LockType.READ,
        () -> {
          try {
            return entityStore.get(
                jobTemplateIdent, Entity.EntityType.JOB_TEMPLATE, JobTemplateEntity.class);
          } catch (NoSuchEntityException e) {
            throw new NoSuchJobTemplateException(
                "Job template with name %s under metalake %s does not exist",
                jobTemplateName, metalake);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public boolean deleteJobTemplate(String metalake, String jobTemplateName) throws InUseException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    List<JobEntity> jobs = listJobs(metalake, Optional.of(jobTemplateName));
    boolean hasActiveJobs =
        jobs.stream()
            .anyMatch(
                job ->
                    job.status() != JobHandle.Status.CANCELLED
                        && job.status() != JobHandle.Status.SUCCEEDED
                        && job.status() != JobHandle.Status.FAILED);
    if (hasActiveJobs) {
      throw new InUseException(
          "Job template %s under metalake %s has active jobs associated with it",
          jobTemplateName, metalake);
    }

    // Delete the job template entity as well as all the jobs associated with it.
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(NamespaceUtil.ofJobTemplate(metalake).levels()),
        LockType.WRITE,
        () -> {
          try {
            return entityStore.delete(
                NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName),
                Entity.EntityType.JOB_TEMPLATE);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public List<JobEntity> listJobs(String metalake, Optional<String> jobTemplateName)
      throws NoSuchJobTemplateException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    Namespace jobNs = NamespaceUtil.ofJob(metalake);
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(jobNs.levels()),
        LockType.READ,
        () -> {
          try {
            // If jobTemplateName is present, check if the job template exists, will throw an
            // exception if the job template does not exist.
            jobTemplateName.ifPresent(s -> getJobTemplate(metalake, s));

            List<JobEntity> jobEntities;
            if (jobTemplateName.isPresent()) {
              NameIdentifier jobTemplateIdent =
                  NameIdentifierUtil.ofJobTemplate(metalake, jobTemplateName.get());

              // Lock the job template to ensure no concurrent modifications/deletions
              jobEntities =
                  TreeLockUtils.doWithTreeLock(
                      jobTemplateIdent,
                      LockType.READ,
                      () ->
                          // List all the jobs associated with the job template
                          entityStore
                              .relationOperations()
                              .listEntitiesByRelation(
                                  SupportsRelationOperations.Type.JOB_TEMPLATE_JOB_REL,
                                  jobTemplateIdent,
                                  Entity.EntityType.JOB_TEMPLATE));
            } else {
              jobEntities = entityStore.list(jobNs, JobEntity.class, Entity.EntityType.JOB);
            }
            return jobEntities;

          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public JobEntity getJob(String metalake, String jobId) throws NoSuchJobException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    NameIdentifier jobIdent = NameIdentifierUtil.ofJob(metalake, jobId);
    return TreeLockUtils.doWithTreeLock(
        jobIdent,
        LockType.READ,
        () -> {
          try {
            return entityStore.get(jobIdent, Entity.EntityType.JOB, JobEntity.class);
          } catch (NoSuchEntityException e) {
            throw new NoSuchJobException(
                "Job with ID %s under metalake %s does not exist", jobId, metalake);
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public JobEntity runJob(String metalake, String jobTemplateName, Map<String, String> jobConf)
      throws NoSuchJobTemplateException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    // Check if the job template exists, will throw NoSuchJobTemplateException if it does not exist.
    JobTemplateEntity jobTemplateEntity = getJobTemplate(metalake, jobTemplateName);

    // Create staging directory.
    // TODO(jerry). The job staging directory will be deleted using a background thread.
    long jobId = idGenerator.nextId();
    String jobStagingPath =
        stagingDir.getAbsolutePath()
            + File.separator
            + metalake
            + File.separator
            + JobHandle.JOB_ID_PREFIX
            + jobId;
    File jobStagingDir = new File(jobStagingPath);
    if (!jobStagingDir.mkdirs()) {
      throw new RuntimeException(
          String.format("Failed to create staging directory %s for job %s", jobStagingDir, jobId));
    }

    // Create a JobTemplate by replacing the template parameters with the jobConf values, and
    // also downloading any necessary files from the URIs specified in the job template.
    JobTemplate jobTemplate = createRuntimeJobTemplate(jobTemplateEntity, jobConf, jobStagingDir);

    // Submit the job template to the job executor
    String jobExecutionId;
    try {
      jobExecutionId = jobExecutor.submitJob(jobTemplate);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to submit job template %s for execution", jobTemplate), e);
    }

    // Create a new JobEntity to represent the job
    JobEntity jobEntity =
        JobEntity.builder()
            .withId(jobId)
            .withJobExecutionId(jobExecutionId)
            .withJobTemplateName(jobTemplateName)
            .withStatus(JobHandle.Status.QUEUED)
            .withNamespace(NamespaceUtil.ofJob(metalake))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      entityStore.put(jobEntity, false /* overwrite */);
    } catch (IOException e) {
      throw new RuntimeException("Failed to register the job entity " + jobEntity, e);
    }

    return jobEntity;
  }

  @Override
  public JobEntity cancelJob(String metalake, String jobId) throws NoSuchJobException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);

    // Retrieve the job entity, will throw NoSuchJobException if the job does not exist.
    JobEntity jobEntity = getJob(metalake, jobId);

    if (jobEntity.status() == JobHandle.Status.CANCELING
        || jobEntity.status() == JobHandle.Status.CANCELLED
        || jobEntity.status() == JobHandle.Status.SUCCEEDED
        || jobEntity.status() == JobHandle.Status.FAILED) {
      // If the job is already cancelling, cancelled, succeeded, or failed, we do not need to cancel
      // it again.
      return jobEntity;
    }

    // Cancel the job using the job executor
    try {
      jobExecutor.cancelJob(jobEntity.jobExecutionId());
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to cancel job with ID %s under metalake %s", jobId, metalake), e);
    }

    // TODO(jerry). Implement a background thread to monitor the job status and update it. Also,
    //  we should delete the finished job entities after a certain period of time.
    // Update the job status to CANCELING
    JobEntity newJobEntity =
        JobEntity.builder()
            .withId(jobEntity.id())
            .withJobExecutionId(jobEntity.jobExecutionId())
            .withJobTemplateName(jobEntity.jobTemplateName())
            .withStatus(JobHandle.Status.CANCELLING)
            .withNamespace(jobEntity.namespace())
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(jobEntity.auditInfo().creator())
                    .withCreateTime(jobEntity.auditInfo().createTime())
                    .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                    .withLastModifiedTime(Instant.now())
                    .build())
            .build();
    return TreeLockUtils.doWithTreeLock(
        NameIdentifierUtil.ofJob(metalake, jobId),
        LockType.WRITE,
        () -> {
          try {
            // Update the job entity in the entity store
            entityStore.put(newJobEntity, true /* overwrite */);
            return newJobEntity;
          } catch (IOException e) {
            throw new RuntimeException(
                String.format("Failed to update job entity %s to CANCELING status", newJobEntity),
                e);
          }
        });
  }

  @Override
  public void close() throws IOException {
    jobExecutor.close();
    // TODO(jerry). Implement any necessary cleanup logic for the JobManager.
  }

  @VisibleForTesting
  public static JobTemplate createRuntimeJobTemplate(
      JobTemplateEntity jobTemplateEntity, Map<String, String> jobConf, File stagingDir) {
    String name = jobTemplateEntity.name();
    String comment = jobTemplateEntity.comment();

    JobTemplateEntity.TemplateContent content = jobTemplateEntity.templateContent();
    String executable = fetchFileFromUri(content.executable(), stagingDir, TIMEOUT_IN_MS);

    List<String> args =
        content.arguments().stream()
            .map(arg -> replacePlaceholder(arg, jobConf))
            .collect(Collectors.toList());
    Map<String, String> environments =
        content.environments().entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> replacePlaceholder(entry.getKey(), jobConf),
                    entry -> replacePlaceholder(entry.getValue(), jobConf)));
    Map<String, String> customFields =
        content.customFields().entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> replacePlaceholder(entry.getKey(), jobConf),
                    entry -> replacePlaceholder(entry.getValue(), jobConf)));

    // For shell job template
    if (content.jobType() == JobTemplate.JobType.SHELL) {
      List<String> scripts = fetchFilesFromUri(content.scripts(), stagingDir, TIMEOUT_IN_MS);

      return ShellJobTemplate.builder()
          .withName(name)
          .withComment(comment)
          .withExecutable(executable)
          .withArguments(args)
          .withEnvironments(environments)
          .withCustomFields(customFields)
          .withScripts(scripts)
          .build();
    }

    // For Spark job template
    if (content.jobType() == JobTemplate.JobType.SPARK) {
      String className = content.className();
      List<String> jars = fetchFilesFromUri(content.jars(), stagingDir, TIMEOUT_IN_MS);
      List<String> files = fetchFilesFromUri(content.files(), stagingDir, TIMEOUT_IN_MS);
      List<String> archives = fetchFilesFromUri(content.archives(), stagingDir, TIMEOUT_IN_MS);
      Map<String, String> configs =
          content.configs().entrySet().stream()
              .collect(
                  Collectors.toMap(
                      entry -> replacePlaceholder(entry.getKey(), jobConf),
                      entry -> replacePlaceholder(entry.getValue(), jobConf)));

      return SparkJobTemplate.builder()
          .withName(name)
          .withComment(comment)
          .withExecutable(executable)
          .withArguments(args)
          .withEnvironments(environments)
          .withCustomFields(customFields)
          .withClassName(className)
          .withJars(jars)
          .withFiles(files)
          .withArchives(archives)
          .withConfigs(configs)
          .build();
    }

    throw new IllegalArgumentException("Unsupported job type: " + content.jobType());
  }

  @VisibleForTesting
  static String replacePlaceholder(String inputString, Map<String, String> replacements) {
    if (StringUtils.isBlank(inputString)) {
      return inputString; // Return as is if the input string is blank
    }

    StringBuffer result = new StringBuffer();

    Matcher matcher = PLACEHOLDER_PATTERN.matcher(inputString);
    while (matcher.find()) {
      String key = matcher.group(1);
      String replacement = replacements.get(key);
      if (replacement != null) {
        matcher.appendReplacement(result, replacement);
      } else {
        // If no replacement is found, keep the placeholder as is
        matcher.appendReplacement(result, matcher.group(0));
      }
    }
    matcher.appendTail(result);

    return result.toString();
  }

  @VisibleForTesting
  static List<String> fetchFilesFromUri(List<String> uris, File stagingDir, int timeoutInMs) {
    return uris.stream()
        .map(uri -> fetchFileFromUri(uri, stagingDir, timeoutInMs))
        .collect(Collectors.toList());
  }

  @VisibleForTesting
  static String fetchFileFromUri(String uri, File stagingDir, int timeoutInMs) {
    try {
      URI fileUri = new URI(uri);
      String scheme = Optional.ofNullable(fileUri.getScheme()).orElse("file");
      File destFile = new File(stagingDir, new File(fileUri.getPath()).getName());

      switch (scheme) {
        case "http":
        case "https":
        case "ftp":
          FileUtils.copyURLToFile(fileUri.toURL(), destFile, timeoutInMs, timeoutInMs);
          break;

        case "file":
          Files.createSymbolicLink(destFile.toPath(), new File(fileUri.getPath()).toPath());
          break;

        default:
          throw new IllegalArgumentException("Unsupported scheme: " + scheme);
      }

      return destFile.getAbsolutePath();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to fetch file from URI %s", uri), e);
    }
  }
}
