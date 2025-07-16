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
package org.apache.gravitino.iceberg.service.rest;

import java.util.Arrays;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Application;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.IcebergCreateNamespaceEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateNamespaceFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateNamespacePreEvent;
import org.apache.gravitino.listener.api.event.IcebergDropNamespaceEvent;
import org.apache.gravitino.listener.api.event.IcebergDropNamespaceFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergDropNamespacePreEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadNamespaceEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadNamespaceFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadNamespacePreEvent;
import org.apache.gravitino.listener.api.event.IcebergNamespaceExistsEvent;
import org.apache.gravitino.listener.api.event.IcebergNamespaceExistsPreEvent;
import org.apache.gravitino.listener.api.event.IcebergRegisterTableEvent;
import org.apache.gravitino.listener.api.event.IcebergRegisterTableFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergRegisterTablePreEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateNamespaceEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateNamespaceFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergUpdateNamespacePreEvent;
import org.apache.iceberg.catalog.Namespace;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

public class TestIcebergNamespaceOperations extends IcebergNamespaceTestBase {

  private DummyEventListener dummyEventListener;

  @Override
  protected Application configure() {
    this.dummyEventListener = new DummyEventListener();
    ResourceConfig resourceConfig =
        IcebergRestTestUtil.getIcebergResourceConfig(
            MockIcebergNamespaceOperations.class, true, Arrays.asList(dummyEventListener));

    // register a mock HttpServletRequest with user info
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
            Mockito.when(mockRequest.getUserPrincipal()).thenReturn(() -> "test-user");
            bind(mockRequest).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  void testCreateNamespace() {
    verifyCreateNamespaceSucc("create_foo1");
    Assertions.assertTrue(
        dummyEventListener.popPreEvent() instanceof IcebergCreateNamespacePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergCreateNamespaceEvent);

    // Already Exists Exception
    verifyCreateNamespaceFail(409, Namespace.of("create_foo1"));
    Assertions.assertTrue(
        dummyEventListener.popPreEvent() instanceof IcebergCreateNamespacePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergCreateNamespaceFailureEvent);

    // multi level namespaces
    verifyCreateNamespaceSucc("create_foo2", "create_foo3");

    verifyCreateNamespaceFail(400, Namespace.of(""));
  }

  @Test
  void testLoadNamespace() {
    verifyCreateNamespaceSucc("load_foo1");
    dummyEventListener.clearEvent();

    verifyLoadNamespaceSucc(Namespace.of("load_foo1"));
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergLoadNamespacePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergLoadNamespaceEvent);

    // load a schema not exists
    verifyLoadNamespaceFail(404, Namespace.of("load_foo2"));
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergLoadNamespacePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergLoadNamespaceFailureEvent);

    verifyCreateNamespaceSucc(Namespace.of("load_foo3", "a"));
    verifyLoadNamespaceSucc(Namespace.of("load_foo3", "a"));
  }

  @Test
  void testNamespaceExists() {
    verifyNamespaceExistsStatusCode(404, Namespace.of("exists_foo1"));
    Assertions.assertTrue(
        dummyEventListener.popPreEvent() instanceof IcebergNamespaceExistsPreEvent);
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertTrue(postEvent instanceof IcebergNamespaceExistsEvent);
    Assertions.assertEquals(false, ((IcebergNamespaceExistsEvent) postEvent).isExists());

    verifyCreateNamespaceSucc("exists_foo1");
    dummyEventListener.clearEvent();
    verifyNamespaceExistsStatusCode(204, Namespace.of("exists_foo1"));
    Assertions.assertTrue(
        dummyEventListener.popPreEvent() instanceof IcebergNamespaceExistsPreEvent);
    postEvent = dummyEventListener.popPostEvent();
    Assertions.assertTrue(postEvent instanceof IcebergNamespaceExistsEvent);
    Assertions.assertEquals(true, ((IcebergNamespaceExistsEvent) postEvent).isExists());

    verifyNamespaceExistsStatusCode(404, Namespace.of("exists_foo2", "a"));
    verifyCreateNamespaceSucc("exists_foo2");
    verifyNamespaceExistsStatusCode(204, Namespace.of("exists_foo2"));
    verifyNamespaceExistsStatusCode(404, Namespace.of("exists_foo2", "a"));
    verifyCreateNamespaceSucc(Namespace.of("exists_foo2", "a"));
    verifyNamespaceExistsStatusCode(204, Namespace.of("exists_foo2"));
    verifyNamespaceExistsStatusCode(204, Namespace.of("exists_foo2", "a"));
    verifyNamespaceExistsStatusCode(404, Namespace.of("exists_foo2", "b"));

    verifyDropNamespaceSucc(Namespace.of("exists_foo2"));
    verifyNamespaceExistsStatusCode(404, Namespace.of("exists_foo2"));
    verifyNamespaceExistsStatusCode(204, Namespace.of("exists_foo2", "a"));

    verifyDropNamespaceSucc(Namespace.of("exists_foo2", "a"));
    verifyNamespaceExistsStatusCode(404, Namespace.of("exists_foo2"));
    verifyNamespaceExistsStatusCode(404, Namespace.of("exists_foo2", "a"));
  }

  @Test
  void testDropNamespace() {
    verifyCreateNamespaceSucc("drop_foo1");
    dummyEventListener.clearEvent();
    verifyDropNamespaceSucc(Namespace.of("drop_foo1"));
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergDropNamespacePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergDropNamespaceEvent);

    verifyLoadNamespaceFail(404, Namespace.of("drop_foo1"));

    // drop fail, no such namespace
    dummyEventListener.clearEvent();
    verifyDropNamespaceFail(404, Namespace.of("drop_foo2"));
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergDropNamespacePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergDropNamespaceFailureEvent);

    // jersery route failed
    verifyDropNamespaceFail(500, Namespace.of(""));

    verifyCreateNamespaceSucc(Namespace.of("drop_foo3", "a"));
    verifyDropNamespaceFail(404, Namespace.of("drop_foo3", "b"));
    verifyDropNamespaceSucc(Namespace.of("drop_foo3", "a"));
  }

  @Test
  void testRegisterTable() {
    verifyRegisterTableSucc("register_foo1", Namespace.of("register_ns"));
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergRegisterTablePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergRegisterTableEvent);

    // Iceberg REST service will throw AlreadyExistsException in test if table name contains 'fail'
    verifyRegisterTableFail(409, "fail_register_foo1", Namespace.of("register_ns"));
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergRegisterTablePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergRegisterTableFailureEvent);

    verifyRegisterTableSucc("register_foo2", Namespace.of("register_ns_2", "a"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", IcebergRestTestUtil.PREFIX})
  void testListNamespace(String prefix) {
    setUrlPathWithPrefix(prefix);
    dropAllExistingNamespace();
    verifyListNamespaceSucc(Optional.empty(), Arrays.asList());

    doCreateNamespace(Namespace.of("list_foo1"));
    doCreateNamespace(Namespace.of("list_foo2"));
    doCreateNamespace(Namespace.of("list_foo3", "a"));
    doCreateNamespace(Namespace.of("list_foo3", "b"));
    doCreateNamespace(Namespace.of("list_foo3", "a", "z"));
    doCreateNamespace(Namespace.of("list_foo3", "a", "y"));

    verifyListNamespaceSucc(Optional.empty(), Arrays.asList("list_foo1", "list_foo2", "list_foo3"));
    verifyListNamespaceSucc(
        Optional.of(Namespace.of("list_foo3")), Arrays.asList("list_foo3.a", "list_foo3.b"));
    verifyListNamespaceSucc(
        Optional.of(Namespace.of("list_foo3", "a")),
        Arrays.asList("list_foo3.a.y", "list_foo3.a.z"));

    verifyListNamespaceFail(Optional.of(Namespace.of("list_fooxx")), 404);
    verifyListNamespaceFail(Optional.of(Namespace.of("list_foo3", "c")), 404);
    verifyListNamespaceFail(Optional.of(Namespace.of("list_foo3", "a", "x")), 404);
  }

  @Test
  void testUpdateNamespace() {
    verifyCreateNamespaceSucc("update_foo1");
    dummyEventListener.clearEvent();
    verifyUpdateNamespaceSucc(Namespace.of("update_foo1"));
    Assertions.assertTrue(
        dummyEventListener.popPreEvent() instanceof IcebergUpdateNamespacePreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergUpdateNamespaceEvent);

    verifyUpdateNamespaceFail(404, Namespace.of("update_foo2"));
    Assertions.assertTrue(
        dummyEventListener.popPreEvent() instanceof IcebergUpdateNamespacePreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergUpdateNamespaceFailureEvent);

    verifyCreateNamespaceSucc(Namespace.of("update_foo3", "a"));
    dummyEventListener.clearEvent();
    verifyUpdateNamespaceSucc(Namespace.of("update_foo3", "a"));
  }
}
