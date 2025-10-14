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

package org.apache.gravitino.listener;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.job.JobTemplateChange;
import org.apache.gravitino.listener.api.event.job.AlterJobTemplateEvent;
import org.apache.gravitino.listener.api.event.job.AlterJobTemplateFailureEvent;
import org.apache.gravitino.listener.api.event.job.AlterJobTemplatePreEvent;
import org.apache.gravitino.listener.api.event.job.DeleteJobTemplateEvent;
import org.apache.gravitino.listener.api.event.job.DeleteJobTemplateFailureEvent;
import org.apache.gravitino.listener.api.event.job.DeleteJobTemplatePreEvent;
import org.apache.gravitino.listener.api.event.job.GetJobTemplateEvent;
import org.apache.gravitino.listener.api.event.job.GetJobTemplateFailureEvent;
import org.apache.gravitino.listener.api.event.job.GetJobTemplatePreEvent;
import org.apache.gravitino.listener.api.event.job.ListJobTemplatesEvent;
import org.apache.gravitino.listener.api.event.job.ListJobTemplatesFailureEvent;
import org.apache.gravitino.listener.api.event.job.ListJobTemplatesPreEvent;
import org.apache.gravitino.listener.api.event.job.RegisterJobTemplateEvent;
import org.apache.gravitino.listener.api.event.job.RegisterJobTemplateFailureEvent;
import org.apache.gravitino.listener.api.event.job.RegisterJobTemplatePreEvent;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.PrincipalUtils;

public class JobEventDispatcher implements JobOperationDispatcher {

  private final EventBus eventBus;
  private final JobOperationDispatcher jobOperationDispatcher;

  public JobEventDispatcher(EventBus eventBus, JobOperationDispatcher jobOperationDispatcher) {
    this.eventBus = eventBus;
    this.jobOperationDispatcher = jobOperationDispatcher;
  }

  @Override
  public List<JobTemplateEntity> listJobTemplates(String metalake) {
    eventBus.dispatchEvent(
        new ListJobTemplatesPreEvent(PrincipalUtils.getCurrentUserName(), metalake));

    try {
      List<JobTemplateEntity> jobTemplates = jobOperationDispatcher.listJobTemplates(metalake);
      eventBus.dispatchEvent(
          new ListJobTemplatesEvent(PrincipalUtils.getCurrentUserName(), metalake));
      return jobTemplates;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListJobTemplatesFailureEvent(PrincipalUtils.getCurrentUserName(), metalake, e));
      throw e;
    }
  }

  @Override
  public void registerJobTemplate(String metalake, JobTemplateEntity jobTemplateEntity)
      throws JobTemplateAlreadyExistsException {
    eventBus.dispatchEvent(
        new RegisterJobTemplatePreEvent(
            PrincipalUtils.getCurrentUserName(), metalake, jobTemplateEntity.toJobTemplate()));

    try {
      jobOperationDispatcher.registerJobTemplate(metalake, jobTemplateEntity);
      eventBus.dispatchEvent(
          new RegisterJobTemplateEvent(
              PrincipalUtils.getCurrentUserName(), metalake, jobTemplateEntity.toJobTemplate()));
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new RegisterJobTemplateFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, jobTemplateEntity.toJobTemplate(), e));
      throw e;
    }
  }

  @Override
  public JobTemplateEntity getJobTemplate(String metalake, String jobTemplateName)
      throws NoSuchJobTemplateException {
    eventBus.dispatchEvent(
        new GetJobTemplatePreEvent(PrincipalUtils.getCurrentUserName(), metalake, jobTemplateName));

    try {
      JobTemplateEntity jobTemplate =
          jobOperationDispatcher.getJobTemplate(metalake, jobTemplateName);
      eventBus.dispatchEvent(
          new GetJobTemplateEvent(
              PrincipalUtils.getCurrentUserName(), metalake, jobTemplate.toJobTemplate()));
      return jobTemplate;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new GetJobTemplateFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, jobTemplateName, e));
      throw e;
    }
  }

  @Override
  public boolean deleteJobTemplate(String metalake, String jobTemplateName) throws InUseException {
    eventBus.dispatchEvent(
        new DeleteJobTemplatePreEvent(
            PrincipalUtils.getCurrentUserName(), metalake, jobTemplateName));

    try {
      boolean result = jobOperationDispatcher.deleteJobTemplate(metalake, jobTemplateName);
      eventBus.dispatchEvent(
          new DeleteJobTemplateEvent(
              PrincipalUtils.getCurrentUserName(), metalake, jobTemplateName, result));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DeleteJobTemplateFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, jobTemplateName, e));
      throw e;
    }
  }

  @Override
  public JobTemplateEntity alterJobTemplate(
      String metalake, String jobTemplateName, JobTemplateChange... changes)
      throws NoSuchJobTemplateException, IllegalArgumentException {
    eventBus.dispatchEvent(
        new AlterJobTemplatePreEvent(
            PrincipalUtils.getCurrentUserName(), metalake, jobTemplateName, changes));

    try {
      JobTemplateEntity updatedJobTemplate =
          jobOperationDispatcher.alterJobTemplate(metalake, jobTemplateName, changes);
      eventBus.dispatchEvent(
          new AlterJobTemplateEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              changes,
              updatedJobTemplate.toJobTemplate()));
      return updatedJobTemplate;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterJobTemplateFailureEvent(
              PrincipalUtils.getCurrentUserName(), metalake, jobTemplateName, changes, e));
      throw e;
    }
  }

  @Override
  public List<JobEntity> listJobs(String metalake, java.util.Optional<String> jobTemplateName)
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
    return jobOperationDispatcher.runJob(metalake, jobTemplateName, jobConf);
  }

  @Override
  public JobEntity cancelJob(String metalake, String jobId) throws NoSuchJobException {
    return jobOperationDispatcher.cancelJob(metalake, jobId);
  }

  @Override
  public void close() throws IOException {
    if (jobOperationDispatcher != null) {
      jobOperationDispatcher.close();
    }
  }
}
