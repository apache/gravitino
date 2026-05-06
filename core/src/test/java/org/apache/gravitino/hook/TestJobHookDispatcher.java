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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.job.JobOperationDispatcher;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.JobTemplateEntity;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestJobHookDispatcher {

  private JobHookDispatcher hookDispatcher;
  private JobOperationDispatcher mockDispatcher;
  private OwnerDispatcher mockOwnerDispatcher;
  // Save the original ownerDispatcher before each test and restore it in tearDown so we do not
  // leak null state into the GravitinoEnv singleton across tests.
  private OwnerDispatcher savedOwnerDispatcher;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    mockDispatcher = mock(JobOperationDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);
    savedOwnerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", mockOwnerDispatcher, true);
    hookDispatcher = new JobHookDispatcher(mockDispatcher);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "ownerDispatcher", savedOwnerDispatcher, true);
  }

  @Test
  public void testRegisterJobTemplateThrowsWhenSetOwnerFails() {
    JobTemplateEntity mockTemplate = mock(JobTemplateEntity.class);
    // Job templates live under the reserved job-template virtual namespace, which is required
    // to have 3 levels (metalake, system-catalog, job-template-schema).
    when(mockTemplate.nameIdentifier())
        .thenReturn(
            NameIdentifier.of(NamespaceUtil.ofJobTemplate("test_metalake"), "test_template"));
    when(mockTemplate.name()).thenReturn("test_template");

    doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> hookDispatcher.registerJobTemplate("test_metalake", mockTemplate));
    Assertions.assertEquals("Set owner failed", thrown.getMessage());
    verify(mockDispatcher).registerJobTemplate(any(), any());
  }

  @Test
  public void testRunJobThrowsWhenSetOwnerFails() {
    JobEntity mockJob = mock(JobEntity.class);
    when(mockJob.nameIdentifier())
        .thenReturn(NameIdentifier.of(NamespaceUtil.ofJob("test_metalake"), "test_job"));
    when(mockJob.name()).thenReturn("test_job");
    when(mockDispatcher.runJob(any(), any(), any())).thenReturn(mockJob);

    doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> hookDispatcher.runJob("test_metalake", "test_template", Collections.emptyMap()));
    Assertions.assertEquals("Set owner failed", thrown.getMessage());
    verify(mockDispatcher).runJob(any(), any(), any());
  }
}
