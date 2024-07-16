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
package org.apache.gravitino.integration.test.trino;

import java.io.Closeable;
import org.apache.gravitino.integration.test.util.CloseableGroup;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CloseableGroupTest {
  @Test
  public void callCloseableTest() throws Exception {
    Closeable closeable1 = Mockito.mock(Closeable.class);
    Closeable closeable2 = Mockito.mock(Closeable.class);
    Closeable closeable3 = Mockito.mock(Closeable.class);

    CloseableGroup closeableGroup = CloseableGroup.create();
    closeableGroup.register(closeable1);
    closeableGroup.register(closeable2);
    closeableGroup.register(closeable3);

    closeableGroup.close();
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
    Mockito.verify(closeable3).close();
  }

  @Test
  public void callAutoCloseableTest() throws Exception {
    Closeable closeable1 = Mockito.mock(Closeable.class);
    AutoCloseable closeable2 = Mockito.mock(AutoCloseable.class);

    CloseableGroup closeableGroup = CloseableGroup.create();
    closeableGroup.register(closeable1);
    closeableGroup.register(closeable2);

    closeableGroup.close();
    Mockito.verify(closeable1).close();
    Mockito.verify(closeable2).close();
  }
}
