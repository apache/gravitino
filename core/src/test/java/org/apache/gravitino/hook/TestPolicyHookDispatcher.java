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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestPolicyHookDispatcher {

  private PolicyHookDispatcher hookDispatcher;
  private PolicyDispatcher mockDispatcher;
  private OwnerDispatcher mockOwnerDispatcher;
  // Save the original ownerDispatcher before each test and restore it in tearDown so we do not
  // leak null state into the GravitinoEnv singleton across tests.
  private OwnerDispatcher savedOwnerDispatcher;
  private GravitinoAuthorizer mockAuthorizer;
  private GravitinoAuthorizer savedAuthorizer;

  @BeforeEach
  public void setUp() throws IllegalAccessException {
    mockDispatcher = mock(PolicyDispatcher.class);
    mockOwnerDispatcher = mock(OwnerDispatcher.class);
    savedOwnerDispatcher = GravitinoEnv.getInstance().internalOwnerDispatcher();
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalOwnerDispatcher", mockOwnerDispatcher, true);
    mockAuthorizer = mock(GravitinoAuthorizer.class);
    savedAuthorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    GravitinoEnv.getInstance().setGravitinoAuthorizer(mockAuthorizer);
    hookDispatcher = new PolicyHookDispatcher(mockDispatcher);
  }

  @AfterEach
  public void tearDown() throws IllegalAccessException {
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalOwnerDispatcher", savedOwnerDispatcher, true);
    GravitinoEnv.getInstance().setGravitinoAuthorizer(savedAuthorizer);
  }

  @Test
  public void testCreatePolicyThrowsWhenSetOwnerFails() {
    PolicyEntity mockPolicy = mock(PolicyEntity.class);
    when(mockDispatcher.createPolicy(any(), any(), any(), any(), anyBoolean(), any()))
        .thenReturn(mockPolicy);

    doThrow(new RuntimeException("Set owner failed"))
        .when(mockOwnerDispatcher)
        .setOwner(any(), any(), any(), any());

    RuntimeException thrown =
        Assertions.assertThrows(
            RuntimeException.class,
            () ->
                hookDispatcher.createPolicy(
                    "test_metalake", "test_policy", null, "comment", true, null));
    Assertions.assertEquals("Set owner failed", thrown.getMessage());
    verify(mockDispatcher).createPolicy(any(), any(), any(), any(), anyBoolean(), any());
  }

  @Test
  public void testDeletePolicyInvalidatesNameIdMapping() {
    when(mockDispatcher.deletePolicy("test_metalake", "test_policy")).thenReturn(true);

    Assertions.assertTrue(hookDispatcher.deletePolicy("test_metalake", "test_policy"));
    verify(mockAuthorizer)
        .handleEntityNameIdMappingChange(
            "test_metalake",
            NameIdentifierUtil.ofPolicy("test_metalake", "test_policy"),
            Entity.EntityType.POLICY);
  }

  @Test
  public void testDeleteMissingPolicyDoesNotInvalidateNameIdMapping() {
    when(mockDispatcher.deletePolicy("test_metalake", "test_policy")).thenReturn(false);

    Assertions.assertFalse(hookDispatcher.deletePolicy("test_metalake", "test_policy"));
    verify(mockAuthorizer, never()).handleEntityNameIdMappingChange(any(), any(), any());
  }

  @Test
  public void testRenamePolicyInvalidatesOldNameIdMapping() {
    PolicyChange[] changes = {
      PolicyChange.updateComment("new comment"), PolicyChange.rename("new_policy")
    };
    PolicyEntity mockPolicy = mock(PolicyEntity.class);
    when(mockDispatcher.alterPolicy("test_metalake", "test_policy", changes))
        .thenReturn(mockPolicy);

    Assertions.assertSame(
        mockPolicy, hookDispatcher.alterPolicy("test_metalake", "test_policy", changes));
    verify(mockAuthorizer)
        .handleEntityNameIdMappingChange(
            "test_metalake",
            NameIdentifierUtil.ofPolicy("test_metalake", "test_policy"),
            Entity.EntityType.POLICY);
  }

  @Test
  public void testAlterPolicyWithoutRenameDoesNotInvalidateNameIdMapping() {
    PolicyChange[] changes = {PolicyChange.updateComment("new comment")};
    when(mockDispatcher.alterPolicy("test_metalake", "test_policy", changes))
        .thenReturn(mock(PolicyEntity.class));

    hookDispatcher.alterPolicy("test_metalake", "test_policy", changes);
    verify(mockAuthorizer, never()).handleEntityNameIdMappingChange(any(), any(), any());
  }

  @Test
  public void testFailedRenamePolicyDoesNotInvalidateNameIdMapping() {
    PolicyChange[] changes = {PolicyChange.rename("new_policy")};
    when(mockDispatcher.alterPolicy("test_metalake", "test_policy", changes))
        .thenThrow(new RuntimeException("Alter failed"));

    Assertions.assertThrows(
        RuntimeException.class,
        () -> hookDispatcher.alterPolicy("test_metalake", "test_policy", changes));
    verify(mockAuthorizer, never()).handleEntityNameIdMappingChange(any(), any(), any());
  }
}
