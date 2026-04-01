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
package org.apache.gravitino.trino.connector.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.trino.spi.connector.ConnectorSession;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestTrinoUserHeaderProvider {

  private TrinoUserHeaderProvider provider;

  @BeforeEach
  public void setUp() {
    provider = new TrinoUserHeaderProvider();
    provider.clearSession();
  }

  @AfterEach
  public void tearDown() {
    provider.clearSession();
  }

  @Test
  public void testGetHeadersWithNoSession() {
    Map<String, String> headers = provider.getHeaders();
    assertTrue(headers.isEmpty(), "Headers should be empty when no session is set");
  }

  @Test
  public void testGetHeadersAfterApplySession() {
    ConnectorSession session = mock(ConnectorSession.class);
    when(session.getUser()).thenReturn("alice");

    provider.applySession(session);

    Map<String, String> headers = provider.getHeaders();
    assertFalse(headers.isEmpty(), "Headers should not be empty after session is applied");
    assertEquals(
        "alice",
        headers.get(TrinoUserHeaderProvider.GRAVITINO_USER_HEADER),
        "X-Gravitino-User header should match session user");
  }

  @Test
  public void testGetHeadersAfterClearSession() {
    ConnectorSession session = mock(ConnectorSession.class);
    when(session.getUser()).thenReturn("bob");

    provider.applySession(session);
    provider.clearSession();

    Map<String, String> headers = provider.getHeaders();
    assertTrue(headers.isEmpty(), "Headers should be empty after session is cleared");
  }

  @Test
  public void testApplySessionOverridesPreviousUser() {
    ConnectorSession session1 = mock(ConnectorSession.class);
    when(session1.getUser()).thenReturn("alice");
    ConnectorSession session2 = mock(ConnectorSession.class);
    when(session2.getUser()).thenReturn("bob");

    provider.applySession(session1);
    provider.applySession(session2);

    Map<String, String> headers = provider.getHeaders();
    assertEquals(
        "bob",
        headers.get(TrinoUserHeaderProvider.GRAVITINO_USER_HEADER),
        "X-Gravitino-User should reflect the most recently applied session user");
  }
}
