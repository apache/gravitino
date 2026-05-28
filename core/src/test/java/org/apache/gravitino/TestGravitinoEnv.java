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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.listener.EventListenerManager;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.storage.relational.EntityChangeLogPoller;
import org.junit.jupiter.api.Test;

/** Tests for {@link GravitinoEnv}. */
public class TestGravitinoEnv {

  @Test
  void testBaseComponentsStartDoesNotStartStaleEntityChangeLogPoller() throws Exception {
    GravitinoEnv env = GravitinoEnv.getInstance();
    MetricsSystem metricsSystem = mock(MetricsSystem.class);
    EventListenerManager eventListenerManager = mock(EventListenerManager.class);
    EntityChangeLogPoller entityChangeLogPoller = mock(EntityChangeLogPoller.class);

    FieldUtils.writeField(env, "metricsSystem", metricsSystem, true);
    FieldUtils.writeField(env, "eventListenerManager", eventListenerManager, true);
    FieldUtils.writeField(env, "entityChangeLogPoller", entityChangeLogPoller, true);
    FieldUtils.writeField(env, "manageFullComponents", false, true);

    env.start();

    verify(metricsSystem).start();
    verify(eventListenerManager).start();
    verify(entityChangeLogPoller, never()).start();
  }
}
