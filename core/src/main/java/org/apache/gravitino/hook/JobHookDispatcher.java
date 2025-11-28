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
package org.apache.gravitino.hook;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.job.JobTemplateChange;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

public class JobHookDispatcher implements JobOperationDispatcher {
  private final JobOperationDispatcher jobOperationDispatcher;

  public JobHookDispatcher(JobOperationDispatcher jobOperationDispatcher) {
    this.jobOperationDispatcher = jobOperationDispatcher;
  }

  @Override
  public List<JobTemplateEntity> listJobTemplates(String metalake) {
    return jobOperationDispatcher.listJobTemplates(metalake);
  }

  @Override
  public void registerJobTemplate(String metalake, JobTemplateEntity jobTemplateEntity)
      throws JobTemplateAlreadyExistsException {
    // Check whether the current user exists or not
    AuthorizationUtils.checkCurrentUser(metalake, PrincipalUtils.getCurrentUserName());

    jobOperationDispatcher.registerJobTemplate(metalake, jobTemplateEntity);

    // Set the creator as the owner of the job template.
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      ownerManager.setOwner(
          metalake,
          NameIdentifierUtil.toMetadataObject(
              jobTemplateEntity.nameIdentifier(), Entity.EntityType.JOB_TEMPLATE),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }
  }

  @Override
  public JobTemplateEntity getJobTemplate(String metalake, String jobTemplateName)
      throws NoSuchJobTemplateException {
    return jobOperationDispatcher.getJobTemplate(metalake, jobTemplateName);
  }

  @Override
  public boolean deleteJobTemplate(String metalake, String jobTemplateName) throws InUseException {
    return jobOperationDispatcher.deleteJobTemplate(metalake, jobTemplateName);
  }

  @Override
  public JobTemplateEntity alterJobTemplate(
      String metalake, String jobTemplateName, JobTemplateChange... changes)
      throws NoSuchJobTemplateException, IllegalArgumentException {
    return jobOperationDispatcher.alterJobTemplate(metalake, jobTemplateName, changes);
  }

  @Override
  public List<JobEntity> listJobs(String metalake, Optional<String> jobTemplateName)
      throws NoSuchJobTemplateException {
    return jobOperationDispatcher.listJobs(metalake, jobTemplateName);
  }

  @Override
  public JobEntity getJob(String metalake, String jobId) throws NoSuchJobException {
    return jobOperationDispatcher.getJob(metalake, jobId);
  }

  @Override
  public JobEntity runJob(String metalake, String jobTemplateName, Map<String, String> jobConf)
      throws NoSuchJobTemplateException {
    // Check whether the current user exists or not
    AuthorizationUtils.checkCurrentUser(metalake, PrincipalUtils.getCurrentUserName());

    JobEntity jobEntity = jobOperationDispatcher.runJob(metalake, jobTemplateName, jobConf);

    // Set the creator as the owner of the job.
    OwnerDispatcher ownerManager = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerManager != null) {
      ownerManager.setOwner(
          metalake,
          NameIdentifierUtil.toMetadataObject(jobEntity.nameIdentifier(), Entity.EntityType.JOB),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }

    return jobEntity;
  }

  @Override
  public JobEntity cancelJob(String metalake, String jobId) throws NoSuchJobException {
    return jobOperationDispatcher.cancelJob(metalake, jobId);
  }

  @Override
  public void close() throws IOException {
    jobOperationDispatcher.close();
  }
}
