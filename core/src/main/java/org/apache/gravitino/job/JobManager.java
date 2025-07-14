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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;

public class JobManager implements JobOperationDispatcher, Closeable {

  private final EntityStore entityStore;

  private final File stagingDir;

  public JobManager(Config config, EntityStore entityStore) {
    this.entityStore = entityStore;

    String stagingDirPath = config.get(Configs.JOB_STAGING_DIR);
    this.stagingDir = new File(stagingDirPath);
    if (stagingDir.exists()) {
      if (!stagingDir.isDirectory()) {
        throw new IllegalArgumentException(
            String.format("Staging directory %s exists but is not a directory", stagingDirPath));
      }

      if (!stagingDir.canExecute() || !stagingDir.canRead() || !stagingDir.canWrite()) {
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

    // TODO. Check if there are any running jobs associated with the job template. If there are
    //  running jobs, throw InUseException

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
  public void close() throws IOException {
    // TODO. Implement any necessary cleanup logic for the JobManager.
  }
}
