/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestClientPool {

  private static final Logger logger = LoggerFactory.getLogger(TestClientPool.class);

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
  void testRun() throws Exception {
    String result = clientPool.run(client -> client.performAction("test"));
    assertEquals("test", result);
  }

  @Test
  void testRunWithException() {
    assertThrows(
        Exception.class,
        () ->
            clientPool.run(
                client -> {
                  throw new Exception("Test exception");
                }));
  }

  @Test
  void testClose() {
    clientPool.close();
    assertTrue(clientPool.isClosed());
  }

  @Test
  void testPoolSize() {
    assertEquals(2, clientPool.poolSize());
  }

  private final class ClientPoolImplExtension extends ClientPoolImpl<ClientMock, Exception> {
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
    private boolean closed = false;

    public String performAction(String input) {
      return input;
    }

    public void close() {
      closed = true;
    }
  }
}
