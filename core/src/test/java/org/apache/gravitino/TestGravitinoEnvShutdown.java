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
package org.apache.gravitino;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.auxiliary.AuxiliaryServiceManager;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

public class TestGravitinoEnvShutdown {

  @Test
  void testAsyncListenersDrainBeforeEntityStoreCloses() throws Exception {
    AtomicBoolean entityStoreClosed = new AtomicBoolean(false);
    EntityStore entityStore = mock(EntityStore.class);
    doAnswer(
            invocation -> {
              entityStoreClosed.set(true);
              return null;
            })
        .when(entityStore)
        .close();
    AuxiliaryServiceManager auxServiceManager = mock(AuxiliaryServiceManager.class);

    // A slow async listener that records whether the entity store was still open while it ran.
    AtomicInteger eventsSeenWithOpenStore = new AtomicInteger();
    EventListenerManager eventListenerManager = new EventListenerManager();
    eventListenerManager.init(new HashMap<>());
    eventListenerManager.addEventListener(
        "slow",
        new EventListenerPlugin() {
          @Override
          public void init(Map<String, String> properties) {}

          @Override
          public void start() {}

          @Override
          public void stop() {}

          @Override
          public void onPostEvent(Event event) {
            try {
              Thread.sleep(200);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            }
            if (!entityStoreClosed.get()) {
              eventsSeenWithOpenStore.incrementAndGet();
            }
          }

          @Override
          public Mode mode() {
            return Mode.ASYNC_ISOLATED;
          }
        });
    eventListenerManager.start();
    eventListenerManager.createEventBus().dispatchEvent(mock(Event.class));

    TestGravitinoEnv env = new TestGravitinoEnv();
    FieldUtils.writeField(env, "entityStore", entityStore, true);
    FieldUtils.writeField(env, "auxServiceManager", auxServiceManager, true);
    FieldUtils.writeField(env, "eventListenerManager", eventListenerManager, true);

    env.shutdown();

    Assertions.assertEquals(1, eventsSeenWithOpenStore.get());
    InOrder order = inOrder(auxServiceManager, entityStore);
    order.verify(auxServiceManager).serviceStop();
    order.verify(entityStore).close();
  }

  private static final class TestGravitinoEnv extends GravitinoEnv {}
}
