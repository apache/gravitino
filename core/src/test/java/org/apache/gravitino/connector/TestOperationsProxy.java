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
package org.apache.gravitino.connector;

import java.security.Principal;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.utils.Executable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link OperationsProxy}. */
public class TestOperationsProxy {

  private static class PassthroughProxyPlugin implements ProxyPlugin {
    @Override
    public Object doAs(
        Principal principal, Executable<Object, Exception> action, Map<String, String> properties)
        throws Throwable {
      return action.execute();
    }

    @Override
    public void bindCatalogOperation(CatalogOperations ops) {}
  }

  @Test
  public void testExceptionsAreUnwrapped() {
    CatalogOperations ops =
        new CatalogOperations() {
          @Override
          public void initialize(
              Map<String, String> config,
              CatalogInfo catalogInfo,
              HasPropertyMetadata hasPropertyMetadata) {}

          @Override
          public void testConnection(NameIdentifier catalogIdent) throws Exception {
            throw new NoSuchTableException("table does not exist");
          }

          @Override
          public void close() {}
        };

    CatalogOperations proxy = OperationsProxy.createProxy(ops, new PassthroughProxyPlugin());

    // The caller must see the operation's own exception, not the reflective
    // InvocationTargetException wrapper, so exception-type dispatch keeps working.
    Assertions.assertThrows(
        NoSuchTableException.class, () -> proxy.testConnection(NameIdentifier.of("catalog")));
  }

  @Test
  public void testNormalInvocationPassesThrough() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    CatalogOperations ops =
        new CatalogOperations() {
          @Override
          public void initialize(
              Map<String, String> config,
              CatalogInfo catalogInfo,
              HasPropertyMetadata hasPropertyMetadata) {}

          @Override
          public void testConnection(NameIdentifier catalogIdent) {
            calls.incrementAndGet();
          }

          @Override
          public void close() {}
        };

    CatalogOperations proxy = OperationsProxy.createProxy(ops, new PassthroughProxyPlugin());
    proxy.testConnection(NameIdentifier.of("catalog"));

    Assertions.assertEquals(1, calls.get());
  }
}
