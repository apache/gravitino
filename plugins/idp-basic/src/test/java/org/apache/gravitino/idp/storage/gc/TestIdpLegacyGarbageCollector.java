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
package org.apache.gravitino.idp.storage.gc;

import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;

import org.apache.gravitino.Config;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIdpLegacyGarbageCollector {

  @AfterEach
  public void tearDown() {
    IdpLegacyGarbageCollectorManager.getInstance().stopForTesting();
  }

  @Test
  public void testCollectAndCleanDoesNotThrow() throws Exception {
    Config config = new Config(false) {};
    config.set(STORE_DELETE_AFTER_TIME, 600000L);

    try (IdpLegacyGarbageCollector collector = new IdpLegacyGarbageCollector(config)) {
      collector.collectAndClean();
    }
  }

  @Test
  public void testManagerStopForTestingWhenNotStarted() {
    Assertions.assertDoesNotThrow(
        () -> IdpLegacyGarbageCollectorManager.getInstance().stopForTesting());
  }
}
