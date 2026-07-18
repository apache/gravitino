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

package org.apache.gravitino.listener.api.event.server;

import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.event.EventSource;
import org.apache.gravitino.listener.api.event.OperationType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestAuthorizationDenialFailureEvent {

  private static final NameIdentifier TABLE_IDENT =
      NameIdentifier.of("metalake1", "catalog1", "schema1", "table1");

  @Test
  public void testFieldsStoredCorrectly() {
    AuthorizationDenialFailureEvent event =
        new AuthorizationDenialFailureEvent("alice", TABLE_IDENT, "loadTable", "TABLE:LOAD");

    Assertions.assertEquals("alice", event.user());
    Assertions.assertEquals(TABLE_IDENT, event.identifier());
    Assertions.assertEquals("loadTable", event.methodName());
    Assertions.assertEquals("TABLE:LOAD", event.expression());
  }

  @Test
  public void testOperationTypeIsAuthorizationDenial() {
    AuthorizationDenialFailureEvent event =
        new AuthorizationDenialFailureEvent("alice", TABLE_IDENT, "loadTable", "TABLE:LOAD");

    Assertions.assertEquals(OperationType.AUTHORIZATION_DENIAL, event.operationType());
  }

  @Test
  public void testEventSourceIsGravitinoServer() {
    AuthorizationDenialFailureEvent event =
        new AuthorizationDenialFailureEvent("alice", TABLE_IDENT, "loadTable", "TABLE:LOAD");

    Assertions.assertEquals(EventSource.GRAVITINO_SERVER, event.eventSource());
  }

  @Test
  public void testCustomInfoContainsAuthFields() {
    AuthorizationDenialFailureEvent event =
        new AuthorizationDenialFailureEvent("alice", TABLE_IDENT, "loadTable", "TABLE:LOAD");

    Map<String, String> info = event.customInfo();
    Assertions.assertEquals("loadTable", info.get("auth.method"));
    Assertions.assertEquals("TABLE:LOAD", info.get("auth.expression"));
    // resource name must NOT be duplicated in customInfo — it is in identifier()
    Assertions.assertFalse(info.containsKey("auth.resource"));
  }

  @Test
  public void testNullExpressionNormalisedToEmptyString() {
    AuthorizationDenialFailureEvent event =
        new AuthorizationDenialFailureEvent("alice", TABLE_IDENT, "loadTable", null);

    Assertions.assertEquals("", event.expression());
    Assertions.assertEquals("", event.customInfo().get("auth.expression"));
  }

  @Test
  public void testNullIdentifierAllowed() {
    // Denial can occur before the resource identifier is resolved (e.g. metalake user validation)
    AuthorizationDenialFailureEvent event =
        new AuthorizationDenialFailureEvent("alice", null, "checkCurrentUser", "");

    Assertions.assertNull(event.identifier());
    Assertions.assertEquals("checkCurrentUser", event.methodName());
  }

  @Test
  public void testExceptionCarriesUserAndMethodContext() {
    AuthorizationDenialFailureEvent event =
        new AuthorizationDenialFailureEvent("alice", TABLE_IDENT, "loadTable", "TABLE:LOAD");

    String message = event.exception().getMessage();
    Assertions.assertTrue(
        message.contains("alice"), "Exception message must reference the denied user");
    Assertions.assertTrue(
        message.contains("loadTable"), "Exception message must reference the denied operation");
  }
}
