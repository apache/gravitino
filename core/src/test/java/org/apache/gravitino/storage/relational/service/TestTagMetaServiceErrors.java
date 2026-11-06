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
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

import java.sql.SQLException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.storage.relational.mapper.TagMetadataObjectRelMapper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.ibatis.exceptions.PersistenceException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class TestTagMetaServiceErrors {
  @Test
  public void testTagIdLookupTranslatesLockTimeout() {
    RuntimeException failure =
        new PersistenceException(new SQLException("Lock timeout", "HYT00", 50200));
    try (MockedStatic<SessionUtils> sessions = mockStatic(SessionUtils.class)) {
      sessions
          .when(
              () ->
                  SessionUtils.doWithCommitAndFetchResult(
                      eq(TagMetadataObjectRelMapper.class), any()))
          .thenThrow(failure);
      OptimisticLockException translated =
          assertThrows(
              OptimisticLockException.class,
              () ->
                  TagMetaService.getInstance()
                      .listAssociatedMetadataObjectsForTagId("metalake", 42L, null));
      assertTrue(translated.getMessage().contains("metalake/tag-id/42"));
    }
  }

  @Test
  public void testTagIdLookupPreservesOtherRuntimeFailures() {
    RuntimeException failure = new IllegalStateException("Unavailable session");
    try (MockedStatic<SessionUtils> sessions = mockStatic(SessionUtils.class)) {
      sessions
          .when(
              () ->
                  SessionUtils.doWithCommitAndFetchResult(
                      eq(TagMetadataObjectRelMapper.class), any()))
          .thenThrow(failure);
      assertSame(
          failure,
          assertThrows(
              IllegalStateException.class,
              () ->
                  TagMetaService.getInstance()
                      .listAssociatedMetadataObjectsForTagId("metalake", 42L, null)));
    }
  }
}
