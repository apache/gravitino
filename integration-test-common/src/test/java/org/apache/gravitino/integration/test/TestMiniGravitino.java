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
package org.apache.gravitino.integration.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.client.RESTClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestMiniGravitino {

  @TempDir private Path mockConfDir;

  @Test
  void testContextLaunchesTheOpenSourceServerByDefault() {
    // Existing callers pass no launcher and must keep getting the open source server.
    MiniGravitinoContext context = new MiniGravitinoContext(Collections.emptyMap(), false, false);

    assertSame(MiniGravitinoContext.DEFAULT_SERVER_LAUNCHER, context.serverLauncher());
  }

  @Test
  void testContextCarriesACustomServerLauncher() throws Exception {
    // A distribution that ships its own entry point initializes components the open source server
    // knows nothing about, so embedded mode has to be able to start that server instead.
    List<String[]> launched = new ArrayList<>();
    MiniGravitinoContext context =
        new MiniGravitinoContext(Collections.emptyMap(), false, false, launched::add);

    context.serverLauncher().launch(new String[] {"gravitino.conf"});

    assertEquals(1, launched.size());
    assertEquals("gravitino.conf", launched.get(0)[0]);
  }

  @Test
  void testStopCleansResourcesWhenServerTaskDoesNotTerminate() throws Exception {
    ExecutorService executor = mock(ExecutorService.class);
    RESTClient restClient = mock(RESTClient.class);
    MiniGravitino miniGravitino = createMiniGravitino(executor, restClient);
    when(executor.awaitTermination(3, TimeUnit.MINUTES)).thenReturn(false);

    RuntimeException exception = assertThrows(RuntimeException.class, miniGravitino::stop);

    assertEquals("Can not terminate MiniGravitino server task", exception.getMessage());
    verify(restClient).close();
    assertFalse(Files.exists(mockConfDir));
  }

  @Test
  void testStopPreservesInterruptionWhenResourceCleanupFails() throws Exception {
    ExecutorService executor = mock(ExecutorService.class);
    RESTClient restClient = mock(RESTClient.class);
    MiniGravitino miniGravitino = createMiniGravitino(executor, restClient);
    InterruptedException interruption = new InterruptedException("interrupted");
    IOException closeFailure = new IOException("close failed");
    when(executor.awaitTermination(3, TimeUnit.MINUTES)).thenThrow(interruption);
    doThrow(closeFailure).when(restClient).close();

    InterruptedException exception = assertThrows(InterruptedException.class, miniGravitino::stop);

    assertSame(interruption, exception);
    assertEquals(1, exception.getSuppressed().length);
    assertSame(closeFailure, exception.getSuppressed()[0]);
    assertFalse(Files.exists(mockConfDir));
  }

  private MiniGravitino createMiniGravitino(ExecutorService executor, RESTClient restClient)
      throws IOException {
    Files.writeString(mockConfDir.resolve("gravitino.conf"), "test");
    return new MiniGravitino(
        new MiniGravitinoContext(Collections.emptyMap(), true, true),
        mockConfDir.toFile(),
        executor,
        restClient);
  }
}
