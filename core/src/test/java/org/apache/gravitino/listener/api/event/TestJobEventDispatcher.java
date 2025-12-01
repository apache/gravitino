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

package org.apache.gravitino.listener.api.event;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.JobTemplateAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchJobException;
import org.apache.gravitino.exceptions.NoSuchJobTemplateException;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.job.JobTemplate;
import org.apache.gravitino.job.JobTemplateChange;
import org.apache.gravitino.listener.DummyEventListener;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.JobEventDispatcher;
import org.apache.gravitino.listener.api.event.job.AlterJobTemplateEvent;
import org.apache.gravitino.listener.api.event.job.AlterJobTemplateFailureEvent;
import org.apache.gravitino.listener.api.event.job.AlterJobTemplatePreEvent;
import org.apache.gravitino.listener.api.event.job.CancelJobEvent;
import org.apache.gravitino.listener.api.event.job.CancelJobFailureEvent;
import org.apache.gravitino.listener.api.event.job.CancelJobPreEvent;
import org.apache.gravitino.listener.api.event.job.DeleteJobTemplateEvent;
import org.apache.gravitino.listener.api.event.job.DeleteJobTemplateFailureEvent;
import org.apache.gravitino.listener.api.event.job.DeleteJobTemplatePreEvent;
import org.apache.gravitino.listener.api.event.job.GetJobEvent;
import org.apache.gravitino.listener.api.event.job.GetJobFailureEvent;
import org.apache.gravitino.listener.api.event.job.GetJobPreEvent;
import org.apache.gravitino.listener.api.event.job.GetJobTemplateEvent;
import org.apache.gravitino.listener.api.event.job.GetJobTemplateFailureEvent;
import org.apache.gravitino.listener.api.event.job.GetJobTemplatePreEvent;
import org.apache.gravitino.listener.api.event.job.ListJobTemplatesEvent;
import org.apache.gravitino.listener.api.event.job.ListJobTemplatesFailureEvent;
import org.apache.gravitino.listener.api.event.job.ListJobTemplatesPreEvent;
import org.apache.gravitino.listener.api.event.job.ListJobsEvent;
import org.apache.gravitino.listener.api.event.job.ListJobsFailureEvent;
import org.apache.gravitino.listener.api.event.job.ListJobsPreEvent;
import org.apache.gravitino.listener.api.event.job.RegisterJobTemplateEvent;
import org.apache.gravitino.listener.api.event.job.RegisterJobTemplateFailureEvent;
import org.apache.gravitino.listener.api.event.job.RegisterJobTemplatePreEvent;
import org.apache.gravitino.listener.api.event.job.RunJobEvent;
import org.apache.gravitino.listener.api.event.job.RunJobFailureEvent;
import org.apache.gravitino.listener.api.event.job.RunJobPreEvent;
import org.apache.gravitino.listener.api.info.JobInfo;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestJobEventDispatcher {
  private JobEventDispatcher failureDispatcher;
  private JobEventDispatcher dispatcher;
  private DummyEventListener dummyEventListener;
  private JobTemplateEntity jobTemplateEntity;
  private JobEntity jobEntity;
  private JobInfo jobInfo;
  private Map<String, String> jobConf;

  @BeforeAll
  void init() {
    this.jobTemplateEntity = mockJobTemplateEntity();
    this.jobEntity = mockJobEntity();
    this.jobInfo = mockJobInfo();
    this.jobConf = Collections.singletonMap("key", "value");
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Collections.singletonList(dummyEventListener));
    JobOperationDispatcher jobExceptionDispatcher = mockExceptionJobDispatcher();
    this.failureDispatcher = new JobEventDispatcher(eventBus, jobExceptionDispatcher);
    JobOperationDispatcher jobDispatcher = mockJobDispatcher();
    this.dispatcher = new JobEventDispatcher(eventBus, jobDispatcher);
  }

  @Test
  void testListJobTemplatesEvent() {
    dispatcher.listJobTemplates("metalake");
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals("metalake", Objects.requireNonNull(preEvent.identifier()).toString());
    Assertions.assertInstanceOf(ListJobTemplatesPreEvent.class, preEvent);
    Assertions.assertEquals(OperationType.LIST_JOB_TEMPLATES, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals("metalake", Objects.requireNonNull(postEvent.identifier()).toString());
    Assertions.assertInstanceOf(ListJobTemplatesEvent.class, postEvent);
    Assertions.assertEquals(OperationType.LIST_JOB_TEMPLATES, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());
  }

  @Test
  void testRegisterJobTemplateEvent() {
    dispatcher.registerJobTemplate("metalake", jobTemplateEntity);
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals(
        NameIdentifierUtil.ofJobTemplate("metalake", jobTemplateEntity.name()),
        Objects.requireNonNull(preEvent.identifier()));
    Assertions.assertInstanceOf(RegisterJobTemplatePreEvent.class, preEvent);
    Assertions.assertEquals(OperationType.REGISTER_JOB_TEMPLATE, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    JobTemplate jobTemplate = ((RegisterJobTemplatePreEvent) preEvent).jobTemplate();
    checkJobTemplate(jobTemplate, jobTemplateEntity);

    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifierUtil.ofJobTemplate("metalake", jobTemplateEntity.name()),
        Objects.requireNonNull(postEvent.identifier()));
    Assertions.assertInstanceOf(RegisterJobTemplateEvent.class, postEvent);
    Assertions.assertEquals(OperationType.REGISTER_JOB_TEMPLATE, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    JobTemplate registeredJobTemplate =
        ((RegisterJobTemplateEvent) postEvent).registeredJobTemplate();
    checkJobTemplate(registeredJobTemplate, jobTemplateEntity);
  }

  @Test
  void testGetJobTemplateEvent() {
    dispatcher.getJobTemplate("metalake", jobTemplateEntity.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals(
        NameIdentifierUtil.ofJobTemplate("metalake", jobTemplateEntity.name()),
        Objects.requireNonNull(preEvent.identifier()));
    Assertions.assertInstanceOf(GetJobTemplatePreEvent.class, preEvent);
    Assertions.assertEquals(OperationType.GET_JOB_TEMPLATE, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifierUtil.ofJobTemplate("metalake", jobTemplateEntity.name()),
        Objects.requireNonNull(postEvent.identifier()));
    Assertions.assertInstanceOf(GetJobTemplateEvent.class, postEvent);
    Assertions.assertEquals(OperationType.GET_JOB_TEMPLATE, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    JobTemplate retrievedJobTemplate = ((GetJobTemplateEvent) postEvent).loadedJobTemplate();
    checkJobTemplate(retrievedJobTemplate, jobTemplateEntity);
  }

  @Test
  void testDeleteJobTemplateEvent() {
    dispatcher.deleteJobTemplate("metalake", jobTemplateEntity.name());
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals(
        NameIdentifierUtil.ofJobTemplate("metalake", jobTemplateEntity.name()),
        Objects.requireNonNull(preEvent.identifier()));
    Assertions.assertInstanceOf(DeleteJobTemplatePreEvent.class, preEvent);
    Assertions.assertEquals(OperationType.DELETE_JOB_TEMPLATE, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifierUtil.ofJobTemplate("metalake", jobTemplateEntity.name()),
        Objects.requireNonNull(postEvent.identifier()));
    Assertions.assertInstanceOf(DeleteJobTemplateEvent.class, postEvent);
    Assertions.assertEquals(OperationType.DELETE_JOB_TEMPLATE, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    boolean isExists = ((DeleteJobTemplateEvent) postEvent).isExists();
    Assertions.assertTrue(isExists);
  }

  @Test
  void testAlterJobTemplateEvent() {
    JobTemplateChange change1 = JobTemplateChange.rename("newName");
    JobTemplateChange[] changes = {change1};

    dispatcher.alterJobTemplate("metalake", jobTemplateEntity.name(), changes);
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals(
        NameIdentifierUtil.ofJobTemplate("metalake", jobTemplateEntity.name()),
        Objects.requireNonNull(preEvent.identifier()));
    Assertions.assertInstanceOf(AlterJobTemplatePreEvent.class, preEvent);
    Assertions.assertEquals(OperationType.ALTER_JOB_TEMPLATE, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    JobTemplateChange[] eventChanges = ((AlterJobTemplatePreEvent) preEvent).changes();
    Assertions.assertArrayEquals(changes, eventChanges);

    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifierUtil.ofJobTemplate("metalake", jobTemplateEntity.name()),
        Objects.requireNonNull(postEvent.identifier()));
    Assertions.assertInstanceOf(AlterJobTemplateEvent.class, postEvent);
    Assertions.assertEquals(OperationType.ALTER_JOB_TEMPLATE, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    JobTemplateChange[] postChanges = ((AlterJobTemplateEvent) postEvent).changes();
    Assertions.assertArrayEquals(changes, postChanges);

    JobTemplate updatedJobTemplate = ((AlterJobTemplateEvent) postEvent).updatedJobTemplate();
    checkJobTemplate(updatedJobTemplate, jobTemplateEntity);
  }

  // Failure event tests
  @Test
  void testRegisterJobTemplateFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.registerJobTemplate("metalake", jobTemplateEntity));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertInstanceOf(RegisterJobTemplateFailureEvent.class, event);
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((RegisterJobTemplateFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.REGISTER_JOB_TEMPLATE, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    JobTemplate jobTemplate = ((RegisterJobTemplateFailureEvent) event).jobTemplate();
    checkJobTemplate(jobTemplate, jobTemplateEntity);
  }

  @Test
  void testGetJobTemplateFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getJobTemplate("metalake", jobTemplateEntity.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertInstanceOf(GetJobTemplateFailureEvent.class, event);
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((GetJobTemplateFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_JOB_TEMPLATE, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testDeleteJobTemplateFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.deleteJobTemplate("metalake", jobTemplateEntity.name()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertInstanceOf(DeleteJobTemplateFailureEvent.class, event);
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((DeleteJobTemplateFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.DELETE_JOB_TEMPLATE, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testAlterJobTemplateFailureEvent() {
    JobTemplateChange change1 = JobTemplateChange.rename("newName");
    JobTemplateChange change2 = JobTemplateChange.updateComment("new comment");
    JobTemplateChange[] changes = new JobTemplateChange[] {change1, change2};

    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.alterJobTemplate("metalake", jobTemplateEntity.name(), changes));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertInstanceOf(AlterJobTemplateFailureEvent.class, event);
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((AlterJobTemplateFailureEvent) event).exception().getClass());
    Assertions.assertEquals(changes, ((AlterJobTemplateFailureEvent) event).changes());
    Assertions.assertEquals(OperationType.ALTER_JOB_TEMPLATE, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testListJobTemplatesFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listJobTemplates("metalake"));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertInstanceOf(ListJobTemplatesFailureEvent.class, event);
    Assertions.assertEquals(
        GravitinoRuntimeException.class,
        ((ListJobTemplatesFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_JOB_TEMPLATES, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  // Job-related event tests
  @Test
  void testListJobsEvent() {
    dispatcher.listJobs("metalake", Optional.empty());
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals("metalake", Objects.requireNonNull(preEvent.identifier()).toString());
    Assertions.assertInstanceOf(ListJobsPreEvent.class, preEvent);
    Assertions.assertEquals(OperationType.LIST_JOBS, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals("metalake", Objects.requireNonNull(postEvent.identifier()).toString());
    Assertions.assertInstanceOf(ListJobsEvent.class, postEvent);
    Assertions.assertEquals(OperationType.LIST_JOBS, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());
  }

  @Test
  void testGetJobEvent() {
    dispatcher.getJob("metalake", jobInfo.jobId());
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals(
        NameIdentifierUtil.ofJob("metalake", jobInfo.jobId()),
        Objects.requireNonNull(preEvent.identifier()));
    Assertions.assertInstanceOf(GetJobPreEvent.class, preEvent);
    Assertions.assertEquals(OperationType.GET_JOB, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifierUtil.ofJob("metalake", jobInfo.jobId()),
        Objects.requireNonNull(postEvent.identifier()));
    Assertions.assertInstanceOf(GetJobEvent.class, postEvent);
    Assertions.assertEquals(OperationType.GET_JOB, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    JobInfo loadedJobInfo = ((GetJobEvent) postEvent).loadedJobInfo();
    checkJobInfo(loadedJobInfo, jobInfo);
  }

  @Test
  void testRunJobEvent() {
    dispatcher.runJob("metalake", jobTemplateEntity.name(), jobConf);
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals(
        NameIdentifierUtil.ofJobTemplate("metalake", jobTemplateEntity.name()),
        Objects.requireNonNull(preEvent.identifier()));
    Assertions.assertInstanceOf(RunJobPreEvent.class, preEvent);
    Assertions.assertEquals(OperationType.RUN_JOB, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    String templateName = ((RunJobPreEvent) preEvent).jobTemplateName();
    Assertions.assertEquals(jobTemplateEntity.name(), templateName);
    Map<String, String> preEventJobConf = ((RunJobPreEvent) preEvent).jobConf();
    Assertions.assertEquals(jobConf, preEventJobConf);

    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifierUtil.ofJob("metalake", jobInfo.jobId()),
        Objects.requireNonNull(postEvent.identifier()));
    Assertions.assertInstanceOf(RunJobEvent.class, postEvent);
    Assertions.assertEquals(OperationType.RUN_JOB, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    JobInfo runJobInfo = ((RunJobEvent) postEvent).runJobInfo();
    checkJobInfo(runJobInfo, jobInfo);
    String postTemplateName = ((RunJobEvent) postEvent).jobTemplateName();
    Assertions.assertEquals(jobTemplateEntity.name(), postTemplateName);
    Map<String, String> postEventJobConf = ((RunJobEvent) postEvent).jobConf();
    Assertions.assertEquals(jobConf, postEventJobConf);
  }

  @Test
  void testCancelJobEvent() {
    dispatcher.cancelJob("metalake", jobInfo.jobId());
    PreEvent preEvent = dummyEventListener.popPreEvent();

    Assertions.assertEquals(
        NameIdentifierUtil.ofJob("metalake", jobInfo.jobId()),
        Objects.requireNonNull(preEvent.identifier()));
    Assertions.assertInstanceOf(CancelJobPreEvent.class, preEvent);
    Assertions.assertEquals(OperationType.CANCEL_JOB, preEvent.operationType());
    Assertions.assertEquals(OperationStatus.UNPROCESSED, preEvent.operationStatus());

    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertEquals(
        NameIdentifierUtil.ofJob("metalake", jobInfo.jobId()),
        Objects.requireNonNull(postEvent.identifier()));
    Assertions.assertInstanceOf(CancelJobEvent.class, postEvent);
    Assertions.assertEquals(OperationType.CANCEL_JOB, postEvent.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, postEvent.operationStatus());

    JobInfo cancelledJobInfo = ((CancelJobEvent) postEvent).cancelledJobInfo();
    checkJobInfo(cancelledJobInfo, jobInfo);
  }

  // Job-related failure event tests
  @Test
  void testListJobsFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.listJobs("metalake", Optional.empty()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertInstanceOf(ListJobsFailureEvent.class, event);
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListJobsFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.LIST_JOBS, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testGetJobFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.getJob("metalake", jobInfo.jobId()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertInstanceOf(GetJobFailureEvent.class, event);
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((GetJobFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.GET_JOB, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  @Test
  void testRunJobFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.runJob("metalake", jobTemplateEntity.name(), jobConf));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertInstanceOf(RunJobFailureEvent.class, event);
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((RunJobFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.RUN_JOB, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());

    String templateName = ((RunJobFailureEvent) event).jobTemplateName();
    Assertions.assertEquals(jobTemplateEntity.name(), templateName);
    Map<String, String> eventJobConf = ((RunJobFailureEvent) event).jobConf();
    Assertions.assertEquals(jobConf, eventJobConf);
  }

  @Test
  void testCancelJobFailureEvent() {
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () -> failureDispatcher.cancelJob("metalake", jobInfo.jobId()));
    Event event = dummyEventListener.popPostEvent();
    Assertions.assertInstanceOf(CancelJobFailureEvent.class, event);
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((CancelJobFailureEvent) event).exception().getClass());
    Assertions.assertEquals(OperationType.CANCEL_JOB, event.operationType());
    Assertions.assertEquals(OperationStatus.FAILURE, event.operationStatus());
  }

  private void checkJobTemplate(JobTemplate actual, JobTemplateEntity expected) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.comment(), actual.comment());
  }

  private void checkJobInfo(JobInfo actual, JobInfo expected) {
    Assertions.assertEquals(expected.jobId(), actual.jobId());
    Assertions.assertEquals(expected.jobTemplateName(), actual.jobTemplateName());
    Assertions.assertEquals(expected.jobStatus(), actual.jobStatus());
  }

  private JobOperationDispatcher mockJobDispatcher() {
    JobOperationDispatcher dispatcher = mock(JobOperationDispatcher.class);
    String metalake = "metalake";

    try {
      // Job template operations
      when(dispatcher.listJobTemplates(metalake))
          .thenReturn(Collections.singletonList(jobTemplateEntity));
      when(dispatcher.getJobTemplate(any(String.class), any(String.class)))
          .thenReturn(jobTemplateEntity);
      when(dispatcher.deleteJobTemplate(metalake, jobTemplateEntity.name())).thenReturn(true);
      when(dispatcher.alterJobTemplate(
              any(String.class), any(String.class), any(JobTemplateChange[].class)))
          .thenReturn(jobTemplateEntity);

      // Job operations
      when(dispatcher.listJobs(any(String.class), any(Optional.class)))
          .thenReturn(Collections.singletonList(jobEntity));
      when(dispatcher.getJob(any(String.class), any(String.class))).thenReturn(jobEntity);
      when(dispatcher.runJob(any(String.class), any(String.class), any(Map.class)))
          .thenReturn(jobEntity);
      when(dispatcher.cancelJob(any(String.class), any(String.class))).thenReturn(jobEntity);
    } catch (JobTemplateAlreadyExistsException
        | NoSuchJobTemplateException
        | NoSuchJobException e) {
      // This shouldn't happen in our mock setup
    }

    return dispatcher;
  }

  private JobTemplateEntity mockJobTemplateEntity() {
    JobTemplateEntity entity = mock(JobTemplateEntity.class);
    when(entity.name()).thenReturn("testJobTemplate");
    when(entity.comment()).thenReturn("test comment");
    when(entity.auditInfo()).thenReturn(mock(AuditInfo.class));

    JobTemplate jobTemplate = mock(JobTemplate.class);
    when(jobTemplate.name()).thenReturn("testJobTemplate");
    when(jobTemplate.comment()).thenReturn("test comment");
    when(entity.toJobTemplate()).thenReturn(jobTemplate);

    return entity;
  }

  private JobOperationDispatcher mockExceptionJobDispatcher() {
    return mock(
        JobOperationDispatcher.class,
        invocation -> {
          throw new GravitinoRuntimeException("Exception for all methods");
        });
  }

  private JobEntity mockJobEntity() {
    JobEntity entity = mock(JobEntity.class);
    when(entity.jobTemplateName()).thenReturn("testJob");
    when(entity.name()).thenReturn("job-12345");
    when(entity.auditInfo()).thenReturn(mock(AuditInfo.class));
    when(entity.status()).thenReturn(JobHandle.Status.SUCCEEDED);

    return entity;
  }

  private JobInfo mockJobInfo() {
    JobInfo info = mock(JobInfo.class);
    when(info.jobId()).thenReturn("job-12345");
    when(info.jobTemplateName()).thenReturn("testJob");
    when(info.jobStatus()).thenReturn(JobHandle.Status.SUCCEEDED);
    return info;
  }
}
