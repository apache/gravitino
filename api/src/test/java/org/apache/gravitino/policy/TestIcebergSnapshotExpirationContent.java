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
package org.apache.gravitino.policy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link IcebergSnapshotExpirationContent}. */
public class TestIcebergSnapshotExpirationContent {

  @Test
  public void testDefaultsAndRules() {
    IcebergSnapshotExpirationContent content =
        (IcebergSnapshotExpirationContent) PolicyContents.icebergSnapshotExpiration();
    assertEquals(IcebergSnapshotExpirationContent.DEFAULT_OLDER_THAN_DAYS, content.olderThanDays());
    assertEquals(IcebergSnapshotExpirationContent.DEFAULT_RETAIN_LAST, content.retainLast());
    assertEquals(IcebergSnapshotExpirationContent.DEFAULT_STREAM_RESULTS, content.streamResults());
    assertEquals("false", content.rules().get(IcebergSnapshotExpirationContent.TRIGGER_EXPR_KEY));
    assertEquals(
        content.olderThanDays(),
        content
            .rules()
            .get(
                IcebergSnapshotExpirationContent.JOB_OPTIONS_PREFIX
                    + IcebergSnapshotExpirationContent.OLDER_THAN_DAYS_KEY));
  }

  @Test
  public void testValidateRejectsInvalidBounds() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PolicyContents.icebergSnapshotExpiration(0L, 1L, false).validate());
    assertThrows(
        IllegalArgumentException.class,
        () -> PolicyContents.icebergSnapshotExpiration(1L, 0L, false).validate());
    IllegalArgumentException tooOld =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                PolicyContents.icebergSnapshotExpiration(
                        IcebergSnapshotExpirationContent.MAX_OLDER_THAN_DAYS + 1, 1L, false)
                    .validate());
    assertTrue(tooOld.getMessage().contains("olderThanDays"));
  }

  @Test
  public void testValidateAcceptsDefaults() {
    PolicyContents.icebergSnapshotExpiration().validate();
    PolicyContents.icebergSnapshotExpiration(5L, 1L, true).validate();
  }
}
