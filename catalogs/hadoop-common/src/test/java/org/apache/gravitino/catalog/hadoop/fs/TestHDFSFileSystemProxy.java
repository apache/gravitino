/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.hadoop.fs;

import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.gravitino.catalog.hadoop.auth.KerberosClient;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Tests for {@link HDFSFileSystemProxy} close lifecycle. */
public class TestHDFSFileSystemProxy {

  /**
   * The private close() hook must stop a present Kerberos client exactly once and make a second
   * close a no-op. On the unfixed tree the hook was empty, so the ticket-refresh executor of every
   * Kerberos-authenticated proxy leaked for the JVM's lifetime.
   */
  @Test
  public void testCloseStopsKerberosClientOnce() throws Exception {
    HDFSFileSystemProxy proxy = new HDFSFileSystemProxy(new Path("file:///"), ImmutableMap.of());

    AtomicInteger closes = new AtomicInteger();
    KerberosClient client = Mockito.mock(KerberosClient.class);
    Mockito.doAnswer(inv -> closes.incrementAndGet()).when(client).close();
    setField(proxy, "kerberosClient", client);

    Method close = HDFSFileSystemProxy.class.getDeclaredMethod("close");
    close.setAccessible(true);
    close.invoke(proxy);
    close.invoke(proxy);

    Assertions.assertEquals(1, closes.get(), "Kerberos client must be closed exactly once");
    Assertions.assertNull(
        getField(proxy, "kerberosClient"), "Second close must find nothing to close");
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Field field = findField(target.getClass(), name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static Object getField(Object target, String name) throws Exception {
    Field field = findField(target.getClass(), name);
    field.setAccessible(true);
    return field.get(target);
  }

  private static Field findField(Class<?> clazz, String name) throws NoSuchFieldException {
    for (Class<?> c = clazz; c != null; c = c.getSuperclass()) {
      try {
        return c.getDeclaredField(name);
      } catch (NoSuchFieldException e) {
        // continue up the hierarchy
      }
    }
    throw new NoSuchFieldException(name);
  }
}
