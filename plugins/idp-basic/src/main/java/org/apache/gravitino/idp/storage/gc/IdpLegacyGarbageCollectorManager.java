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

import java.io.IOException;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the lifecycle of the built-in IdP legacy garbage collector. */
public final class IdpLegacyGarbageCollectorManager {
  private static final Logger LOG = LoggerFactory.getLogger(IdpLegacyGarbageCollectorManager.class);
  private static final IdpLegacyGarbageCollectorManager INSTANCE =
      new IdpLegacyGarbageCollectorManager();

  private IdpLegacyGarbageCollector garbageCollector;

  private IdpLegacyGarbageCollectorManager() {}

  public static IdpLegacyGarbageCollectorManager getInstance() {
    return INSTANCE;
  }

  public synchronized void ensureStarted() {
    if (garbageCollector != null) {
      return;
    }

    try {
      Config config = GravitinoEnv.getInstance().config();
      garbageCollector = new IdpLegacyGarbageCollector(config);
      garbageCollector.start();
    } catch (Exception e) {
      LOG.warn("Failed to start built-in IdP legacy garbage collector", e);
    }
  }

  public synchronized void close() throws IOException {
    if (garbageCollector == null) {
      return;
    }

    try {
      garbageCollector.close();
    } finally {
      garbageCollector = null;
    }
  }
}
