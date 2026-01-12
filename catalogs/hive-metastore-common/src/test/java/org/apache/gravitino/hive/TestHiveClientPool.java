/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Properties;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.hive.client.HiveClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Referenced from Apache Iceberg's {@code TestHiveClientPool} implementation.
 *
 * <p>Source: hive-metastore/src/test/java/org/apache/iceberg/hive/TestHiveClientPool.java
 */
public class TestHiveClientPool {

  private HiveClientPool clients;

  @BeforeEach
  public void before() {
    HiveClientPool clientPool = new HiveClientPool("hive", 2, new Properties());
    clients = Mockito.spy(clientPool);
  }

  @AfterEach
  public void after() {
    clients.close();
    clients = null;
  }

  @Test
  public void testNewClientFailure() {
    Mockito.doThrow(new RuntimeException("Connection exception")).when(clients).newClient();
    RuntimeException ex = assertThrows(RuntimeException.class, () -> clients.run(Object::toString));
    assertEquals("Connection exception", ex.getMessage());
  }

  @Test
  public void testReconnect() {
    HiveClient hiveClient = newClient();

    String metaMessage = "Got exception: org.apache.thrift.transport.TTransportException";
    Mockito.doThrow(new GravitinoRuntimeException(metaMessage))
        .when(hiveClient)
        .getAllDatabases("");

    GravitinoRuntimeException ex =
        assertThrows(
            GravitinoRuntimeException.class,
            () -> clients.run(client -> client.getAllDatabases("")));
    assertEquals("Got exception: org.apache.thrift.transport.TTransportException", ex.getMessage());
    // Verify that the method is never called.
    Mockito.verify(clients, Mockito.never()).reconnect(hiveClient);
  }

  @Test
  public void testClose() throws Exception {
    HiveClient hiveClient = newClient();

    List<String> databases = Lists.newArrayList("db1", "db2");
    Mockito.doReturn(databases).when(hiveClient).getAllDatabases("");
    assertEquals(clients.run(client -> client.getAllDatabases("")), databases);

    clients.close();
    assertTrue(clients.isClosed());
    Mockito.verify(hiveClient).close();
  }

  private HiveClient newClient() {
    HiveClient hiveClient = Mockito.mock(HiveClient.class);
    Mockito.doReturn(hiveClient).when(clients).newClient();
    return hiveClient;
  }
}
