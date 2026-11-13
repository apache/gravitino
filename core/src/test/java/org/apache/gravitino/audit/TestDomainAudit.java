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
package org.apache.gravitino.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.audit.v2.SimpleFormatterV2;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.listener.DomainEventDispatcher;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.domain.AttachDomainEvent;
import org.apache.gravitino.listener.api.event.domain.AttachDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.DomainEvent;
import org.apache.gravitino.listener.api.info.DomainInfo;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.RequestContext;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

class TestDomainAudit {
  @Test
  void testParentChangeAuditIncludesRequestedParent() throws Exception {
    for (String parent : new String[] {"Finance", "Risk"}) {
      DomainInfo info =
          new DomainInfo(
              "Trading", null, Collections.emptyMap(), Domain.Status.PUBLISHED, parent, null, null);
      for (Event event :
          Arrays.asList(
              new AttachDomainEvent("user", "lake", "Trading", parent, info),
              new AttachDomainFailureEvent(
                  "user",
                  "lake",
                  "Trading",
                  parent,
                  new IllegalArgumentException("parent change rejected")))) {
        JsonNode json =
            JsonUtils.objectMapper().readTree(new JsonAuditFormatter().format(event).toString());
        assertEquals(parent, json.get("customInfo").get("parent").asText());
        assertTrue(new SimpleFormatterV2().format(event).toString().contains("parent=" + parent));
      }
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  void testCustomDomainEventWithoutKnownOperationRetainsUnknownFallback() {
    DomainEvent event = new DomainEvent("user", NameIdentifier.of("lake")) {};
    assertEquals(
        AuditLog.Operation.UNKNOWN_OPERATION, new SimpleFormatter().format(event).operation());
    assertEquals(
        AuditLog.Operation.UNKNOWN_OPERATION, new SimpleFormatterV2().format(event).operation());
  }

  @Test
  @SuppressWarnings("deprecation")
  void testExistingAuditListenerWritesDomainSuccessAndFailure() {
    Config config = new Config(false) {};
    config.set(Configs.AUDIT_LOG_ENABLED_CONF, true);
    config.set(Configs.AUDIT_LOG_WRITER_CLASS_NAME, DummyAuditWriter.class.getName());
    config.set(Configs.AUDIT_LOG_FORMATTER_CLASS_NAME, DummyAuditFormatter.class.getName());
    EventListenerManager listeners = new EventListenerManager();
    listeners.init(Collections.emptyMap());
    AuditLogManager audit = new AuditLogManager();
    audit.init(config, listeners);
    listeners.start();
    try {
      DomainDispatcher delegate = mock(DomainDispatcher.class);
      when(delegate.listDomains("lake")).thenReturn(new String[] {"Finance"});
      when(delegate.getDomain("lake", "hidden")).thenThrow(new NoSuchDomainException("hidden"));
      DomainEventDispatcher dispatcher =
          new DomainEventDispatcher(listeners.createEventBus(), delegate);
      dispatcher.listDomains("lake");
      assertThrows(NoSuchDomainException.class, () -> dispatcher.getDomain("lake", "hidden"));
      DummyAuditWriter writer = (DummyAuditWriter) audit.getAuditLogWriter();
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .until(() -> writer.getAuditLogs().size() == 2);
      assertEquals(AuditLog.Operation.LIST_DOMAIN, writer.getAuditLogs().get(0).operation());
      assertEquals(AuditLog.Status.SUCCESS, writer.getAuditLogs().get(0).status());
      assertEquals(AuditLog.Operation.GET_DOMAIN, writer.getAuditLogs().get(1).operation());
      assertEquals(AuditLog.Status.FAILURE, writer.getAuditLogs().get(1).status());
      assertEquals(
          NameIdentifierUtil.ofDomain("lake", "hidden").toString(),
          writer.getAuditLogs().get(1).identifier());
    } finally {
      listeners.stop();
      RequestContext.clear();
    }
  }
}
