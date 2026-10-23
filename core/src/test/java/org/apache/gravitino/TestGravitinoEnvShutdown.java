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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.metrics.MetricsSystem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoEnvShutdown {

  @Test
  void testShutdownClearsComponentsSetByEarlierCode() throws IllegalAccessException {
    TestGravitinoEnv env = new TestGravitinoEnv();
    env.setGravitinoAuthorizer(mock(GravitinoAuthorizer.class));
    FieldUtils.writeField(
        env, "internalAccessControlDispatcher", mock(AccessControlDispatcher.class), true);
    FieldUtils.writeField(env, "metricsSystem", new MetricsSystem(), true);

    env.shutdown();

    Assertions.assertNull(env.gravitinoAuthorizer());
    Assertions.assertNull(env.internalAccessControlDispatcher());
    Assertions.assertNull(env.metricsSystem());
  }

  private static final class TestGravitinoEnv extends GravitinoEnv {}
}
