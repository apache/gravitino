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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.exceptions.IllegalJobTemplateOperationException;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;

/**
 * {@code JobTemplateValidationDispatcher} is a decorator for {@link JobOperationDispatcher} that
 * enforces validation rules for job template operations, specifically protecting built-in job
 * templates from being modified or deleted by users.
 *
 * <p>Built-in job templates are identified by names starting with {@link
 * JobTemplateProvider#BUILTIN_NAME_PREFIX}. These templates are managed by the system and cannot be
 * created, altered, or deleted through user operations.
 *
 * <p>This dispatcher ensures that:
 *
 * <ul>
 *   <li>Users cannot register job templates with names starting with "builtin-"
 *   <li>Users cannot alter built-in job templates
 *   <li>Users cannot delete built-in job templates
 *   <li>Users cannot rename job templates to names starting with "builtin-"
 * </ul>
 */
public class JobTemplateValidationDispatcher implements JobOperationDispatcher {

  private final JobOperationDispatcher dispatcher;

  /**
   * Creates a new JobTemplateValidationDispatcher.
   *
   * @param dispatcher the underlying dispatcher to delegate operations to
   */
  public JobTemplateValidationDispatcher(JobOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public List<JobTemplateEntity> listJobTemplates(String metalake) {
    return dispatcher.listJobTemplates(metalake);
  }

  @Override
  public void registerJobTemplate(String metalake, JobTemplateEntity jobTemplateEntity)
      throws JobTemplateAlreadyExistsException {
    validateNotBuiltInTemplateName(jobTemplateEntity.name());
    dispatcher.registerJobTemplate(metalake, jobTemplateEntity);
  }

  @Override
  public JobTemplateEntity getJobTemplate(String metalake, String jobTemplateName)
      throws NoSuchJobTemplateException {
    return dispatcher.getJobTemplate(metalake, jobTemplateName);
  }

  @Override
  public boolean deleteJobTemplate(String metalake, String jobTemplateName) throws InUseException {
    validateNotBuiltInTemplate(jobTemplateName, "delete");
    return dispatcher.deleteJobTemplate(metalake, jobTemplateName);
  }

  @Override
  public JobTemplateEntity alterJobTemplate(
      String metalake, String jobTemplateName, JobTemplateChange... changes)
      throws NoSuchJobTemplateException, IllegalArgumentException {
    validateNotBuiltInTemplate(jobTemplateName, "alter");

    // Check if any rename operation tries to rename to a built-in name
    Optional<String> newName =
        Arrays.stream(changes)
            .filter(c -> c instanceof JobTemplateChange.RenameJobTemplate)
            .map(c -> ((JobTemplateChange.RenameJobTemplate) c).getNewName())
            .reduce((first, second) -> second);

    if (newName.isPresent()) {
      validateNotBuiltInTemplateName(newName.get());
    }

    return dispatcher.alterJobTemplate(metalake, jobTemplateName, changes);
  }

  @Override
  public List<JobEntity> listJobs(String metalake, Optional<String> jobTemplateName)
      throws NoSuchJobTemplateException {
    return dispatcher.listJobs(metalake, jobTemplateName);
  }

  @Override
  public JobEntity getJob(String metalake, String jobId) throws NoSuchJobException {
    return dispatcher.getJob(metalake, jobId);
  }

  @Override
  public JobEntity runJob(String metalake, String jobTemplateName, Map<String, String> jobConf)
      throws NoSuchJobTemplateException {
    return dispatcher.runJob(metalake, jobTemplateName, jobConf);
  }

  @Override
  public JobEntity cancelJob(String metalake, String jobId) throws NoSuchJobException {
    return dispatcher.cancelJob(metalake, jobId);
  }

  @Override
  public void close() throws IOException {
    dispatcher.close();
  }

  /**
   * Validates that the given job template name does not start with the built-in prefix.
   *
   * @param templateName the job template name to validate
   * @throws IllegalJobTemplateOperationException if the name starts with the built-in prefix
   */
  @VisibleForTesting
  void validateNotBuiltInTemplateName(String templateName) {
    if (isBuiltInTemplateName(templateName)) {
      throw new IllegalJobTemplateOperationException(
          "Job template name '%s' is reserved for built-in templates. "
              + "User-created job templates cannot have names starting with '%s'",
          templateName, JobTemplateProvider.BUILTIN_NAME_PREFIX);
    }
  }

  /**
   * Validates that the given job template is not a built-in template before performing the
   * specified operation.
   *
   * @param templateName the job template name to validate
   * @param operation the operation being attempted (e.g., "delete", "alter")
   * @throws IllegalJobTemplateOperationException if the template is a built-in template
   */
  @VisibleForTesting
  void validateNotBuiltInTemplate(String templateName, String operation) {
    if (isBuiltInTemplateName(templateName)) {
      throw new IllegalJobTemplateOperationException(
          "Cannot %s built-in job template '%s'. "
              + "Built-in job templates are managed by the system and cannot be modified or deleted"
              + " by users",
          operation, templateName);
    }
  }

  /**
   * Checks if the given template name is a built-in template name.
   *
   * @param templateName the template name to check
   * @return true if the name starts with the built-in prefix, false otherwise
   */
  @VisibleForTesting
  static boolean isBuiltInTemplateName(String templateName) {
    return templateName != null && templateName.startsWith(JobTemplateProvider.BUILTIN_NAME_PREFIX);
  }
}
