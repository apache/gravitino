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

import com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.GravitinoEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Lazily starts the built-in IdP legacy garbage collector when mappers are registered. */
public final class IdpLegacyGarbageCollectorManager {

  private static final Logger LOG = LoggerFactory.getLogger(IdpLegacyGarbageCollectorManager.class);
  private static final IdpLegacyGarbageCollectorManager INSTANCE =
      new IdpLegacyGarbageCollectorManager();

  private volatile IdpLegacyGarbageCollector garbageCollector;

  private IdpLegacyGarbageCollectorManager() {}

  /**
   * Returns the singleton manager.
   *
   * @return the manager instance
   */
  public static IdpLegacyGarbageCollectorManager getInstance() {
    return INSTANCE;
  }

  /**
   * Ensures the built-in IdP garbage collector is started once relational mappers are available.
   */
  public synchronized void ensureStarted() {
    if (garbageCollector != null) {
      return;
    }

    try {
      IdpLegacyGarbageCollector collector =
          new IdpLegacyGarbageCollector(GravitinoEnv.getInstance().config());
      collector.start();
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      collector.close();
                    } catch (Exception closeException) {
                      LOG.warn(
                          "Failed to stop built-in IdP legacy garbage collector on shutdown",
                          closeException);
                    }
                  },
                  "idp-legacy-gc-shutdown"));
      garbageCollector = collector;
    } catch (Exception e) {
      LOG.warn("Failed to start built-in IdP legacy garbage collector", e);
    }
  }

  /** Stops the collector. Intended for unit tests only. */
  @VisibleForTesting
  public synchronized void stopForTesting() {
    if (garbageCollector == null) {
      return;
    }
    try {
      garbageCollector.close();
    } catch (Exception e) {
      LOG.warn("Failed to stop built-in IdP legacy garbage collector during testing", e);
    } finally {
      garbageCollector = null;
    }
  }
}
