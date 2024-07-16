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
package org.apache.gravitino.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestClientPool {

  private ClientPoolImpl<ClientMock, Exception> clientPool;

  @BeforeEach
  public void setUp() {
    clientPool = new ClientPoolImplExtension(2, Exception.class, true);
  }

  @AfterEach
  public void tearDown() {
    clientPool.close();
  }

  @Test
  public void testRun() throws Exception {
    String result = clientPool.run(client -> client.performAction("test"));
    assertEquals("test", result);
  }

  @Test
  public void testRunWithException() {
    assertThrows(
        Exception.class,
        () ->
            clientPool.run(
                client -> {
                  throw new Exception("Test exception");
                }));
  }

  @Test
  public void testClose() {
    clientPool.close();
    assertTrue(clientPool.isClosed());
  }

  @Test
  public void testPoolSize() {
    assertEquals(2, clientPool.poolSize());
  }

  private static final class ClientPoolImplExtension extends ClientPoolImpl<ClientMock, Exception> {
    private ClientPoolImplExtension(
        int poolSize, Class<? extends Exception> reconnectExc, boolean retryByDefault) {
      super(poolSize, reconnectExc, retryByDefault);
    }

    @Override
    protected ClientMock newClient() {
      return new ClientMock();
    }

    @Override
    protected ClientMock reconnect(ClientMock client) {
      return client;
    }

    @Override
    protected boolean isConnectionException(Exception exc) {
      return false;
    }

    @Override
    protected void close(ClientMock client) {
      client.close();
    }
  }

  private static class ClientMock {

    public String performAction(String input) {
      return input;
    }

    public void close() {}
  }
}
