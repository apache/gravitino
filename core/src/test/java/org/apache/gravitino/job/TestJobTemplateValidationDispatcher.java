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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalJobTemplateOperationException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestJobTemplateValidationDispatcher {

  private JobOperationDispatcher mockDispatcher;
  private JobTemplateValidationDispatcher validationDispatcher;

  @BeforeEach
  public void setUp() {
    mockDispatcher = mock(JobOperationDispatcher.class);
    validationDispatcher = new JobTemplateValidationDispatcher(mockDispatcher);
  }

  @Test
  public void testIsBuiltInTemplateName() {
    // Test built-in template names
    assertTrue(JobTemplateValidationDispatcher.isBuiltInTemplateName("builtin-spark-pi"));
    assertTrue(JobTemplateValidationDispatcher.isBuiltInTemplateName("builtin-test"));
    assertTrue(JobTemplateValidationDispatcher.isBuiltInTemplateName("builtin-"));

    // Test non-built-in template names
    assertFalse(JobTemplateValidationDispatcher.isBuiltInTemplateName("my-template"));
    assertFalse(JobTemplateValidationDispatcher.isBuiltInTemplateName("user-template"));
    assertFalse(JobTemplateValidationDispatcher.isBuiltInTemplateName("BUILTIN-test"));
    assertFalse(JobTemplateValidationDispatcher.isBuiltInTemplateName(""));
    assertFalse(JobTemplateValidationDispatcher.isBuiltInTemplateName(null));
  }

  @Test
  public void testRegisterJobTemplateWithBuiltInName() {
    JobTemplateEntity entity = createJobTemplateEntity("builtin-test-template");

    IllegalJobTemplateOperationException exception =
        assertThrows(
            IllegalJobTemplateOperationException.class,
            () -> validationDispatcher.registerJobTemplate("metalake1", entity));

    assertTrue(exception.getMessage().contains("builtin-test-template"));
    assertTrue(exception.getMessage().contains("reserved for built-in templates"));
    verify(mockDispatcher, never()).registerJobTemplate(anyString(), any());
  }

  @Test
  public void testRegisterJobTemplateWithUserName() throws JobTemplateAlreadyExistsException {
    JobTemplateEntity entity = createJobTemplateEntity("my-custom-template");

    doNothing().when(mockDispatcher).registerJobTemplate(eq("metalake1"), eq(entity));

    assertDoesNotThrow(() -> validationDispatcher.registerJobTemplate("metalake1", entity));

    verify(mockDispatcher).registerJobTemplate("metalake1", entity);
  }

  @Test
  public void testDeleteBuiltInJobTemplate() {
    IllegalJobTemplateOperationException exception =
        assertThrows(
            IllegalJobTemplateOperationException.class,
            () -> validationDispatcher.deleteJobTemplate("metalake1", "builtin-spark-pi"));

    assertTrue(exception.getMessage().contains("Cannot delete"));
    assertTrue(exception.getMessage().contains("builtin-spark-pi"));
    assertTrue(exception.getMessage().contains("managed by the system"));
    verify(mockDispatcher, never()).deleteJobTemplate(anyString(), anyString());
  }

  @Test
  public void testDeleteUserJobTemplate() {
    when(mockDispatcher.deleteJobTemplate("metalake1", "my-template")).thenReturn(true);

    boolean result = validationDispatcher.deleteJobTemplate("metalake1", "my-template");

    assertTrue(result);
    verify(mockDispatcher).deleteJobTemplate("metalake1", "my-template");
  }

  @Test
  public void testAlterBuiltInJobTemplate() {
    JobTemplateChange change = JobTemplateChange.updateComment("new comment");

    IllegalJobTemplateOperationException exception =
        assertThrows(
            IllegalJobTemplateOperationException.class,
            () -> validationDispatcher.alterJobTemplate("metalake1", "builtin-spark-pi", change));

    assertTrue(exception.getMessage().contains("Cannot alter"));
    assertTrue(exception.getMessage().contains("builtin-spark-pi"));
    verify(mockDispatcher, never())
        .alterJobTemplate(anyString(), anyString(), any(JobTemplateChange[].class));
  }

  @Test
  public void testAlterUserJobTemplate() {
    JobTemplateEntity entity = createJobTemplateEntity("my-template");
    JobTemplateChange change = JobTemplateChange.updateComment("new comment");

    when(mockDispatcher.alterJobTemplate("metalake1", "my-template", change)).thenReturn(entity);

    JobTemplateEntity result =
        validationDispatcher.alterJobTemplate("metalake1", "my-template", change);

    assertEquals(entity, result);
    verify(mockDispatcher).alterJobTemplate("metalake1", "my-template", change);
  }

  @Test
  public void testRenameToBuiltInName() {
    JobTemplateChange change = JobTemplateChange.rename("builtin-new-name");

    IllegalJobTemplateOperationException exception =
        assertThrows(
            IllegalJobTemplateOperationException.class,
            () -> validationDispatcher.alterJobTemplate("metalake1", "my-template", change));

    assertTrue(exception.getMessage().contains("builtin-new-name"));
    assertTrue(exception.getMessage().contains("reserved for built-in templates"));
    verify(mockDispatcher, never())
        .alterJobTemplate(anyString(), anyString(), any(JobTemplateChange[].class));
  }

  @Test
  public void testRenameToUserName() {
    JobTemplateEntity entity = createJobTemplateEntity("new-user-name");
    JobTemplateChange change = JobTemplateChange.rename("new-user-name");

    when(mockDispatcher.alterJobTemplate("metalake1", "old-name", change)).thenReturn(entity);

    JobTemplateEntity result =
        validationDispatcher.alterJobTemplate("metalake1", "old-name", change);

    assertEquals(entity, result);
    verify(mockDispatcher).alterJobTemplate("metalake1", "old-name", change);
  }

  @Test
  public void testMultipleChangesWithBuiltInRename() {
    JobTemplateChange change1 = JobTemplateChange.updateComment("new comment");
    JobTemplateChange change2 = JobTemplateChange.rename("builtin-new-name");

    IllegalJobTemplateOperationException exception =
        assertThrows(
            IllegalJobTemplateOperationException.class,
            () ->
                validationDispatcher.alterJobTemplate(
                    "metalake1", "my-template", change1, change2));

    assertTrue(exception.getMessage().contains("builtin-new-name"));
    verify(mockDispatcher, never())
        .alterJobTemplate(anyString(), anyString(), any(JobTemplateChange[].class));
  }

  @Test
  public void testListJobTemplates() {
    validationDispatcher.listJobTemplates("metalake1");
    verify(mockDispatcher).listJobTemplates("metalake1");
  }

  @Test
  public void testGetJobTemplate() {
    validationDispatcher.getJobTemplate("metalake1", "any-template");
    verify(mockDispatcher).getJobTemplate("metalake1", "any-template");
  }

  @Test
  public void testListJobs() {
    validationDispatcher.listJobs("metalake1", Optional.empty());
    verify(mockDispatcher).listJobs("metalake1", Optional.empty());
  }

  @Test
  public void testGetJob() {
    validationDispatcher.getJob("metalake1", "job-123");
    verify(mockDispatcher).getJob("metalake1", "job-123");
  }

  @Test
  public void testRunJob() {
    validationDispatcher.runJob("metalake1", "my-template", Collections.emptyMap());
    verify(mockDispatcher).runJob("metalake1", "my-template", Collections.emptyMap());
  }

  @Test
  public void testCancelJob() {
    validationDispatcher.cancelJob("metalake1", "job-123");
    verify(mockDispatcher).cancelJob("metalake1", "job-123");
  }

  private JobTemplateEntity createJobTemplateEntity(String name) {
    return JobTemplateEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(Namespace.of("metalake1"))
        .withComment("test template")
        .withTemplateContent(
            JobTemplateEntity.TemplateContent.builder()
                .withJobType(JobTemplate.JobType.SPARK)
                .withExecutable("spark-submit")
                .withArguments(Collections.emptyList())
                .withEnvironments(Collections.emptyMap())
                .withCustomFields(Collections.emptyMap())
                .withClassName("com.example.Main")
                .withJars(Collections.emptyList())
                .withFiles(Collections.emptyList())
                .withArchives(Collections.emptyList())
                .withConfigs(Collections.emptyMap())
                .build())
        .withAuditInfo(
            AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
        .build();
  }
}
