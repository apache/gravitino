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

import java.time.Instant;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.listener.api.event.job.DeleteJobEvent;
import org.apache.gravitino.listener.api.info.JobInfo;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The staging directory cleanup is the only path that deletes a job, and it runs on the server's
 * own schedule rather than for a request, so this event is the only signal a listener gets that a
 * job is gone.
 */
class TestDeleteJobEvent {

  @Test
  void testEventCarriesTheDeletedJob() {
    JobEntity job =
        JobEntity.builder()
            .withId(9001L)
            .withJobExecutionId("exec-1")
            .withNamespace(NamespaceUtil.ofJob("metalake"))
            .withJobTemplateName("daily-etl")
            .withStatus(JobHandle.Status.SUCCEEDED)
            .withStartedAt(System.currentTimeMillis())
            .withFinishedAt(System.currentTimeMillis())
            .withAuditInfo(
                AuditInfo.builder().withCreator("tester").withCreateTime(Instant.now()).build())
            .build();
    JobInfo jobInfo = JobInfo.fromJobEntity(job);

    DeleteJobEvent event = new DeleteJobEvent(DeleteJobEvent.SYSTEM_USER, "metalake", jobInfo);

    Assertions.assertEquals(NameIdentifierUtil.ofJob("metalake", job.name()), event.identifier());
    Assertions.assertEquals(OperationType.DELETE_JOB, event.operationType());
    Assertions.assertEquals(OperationStatus.SUCCESS, event.operationStatus());
    Assertions.assertSame(jobInfo, event.deletedJobInfo());
    Assertions.assertEquals(DeleteJobEvent.SYSTEM_USER, event.user());
  }
}
