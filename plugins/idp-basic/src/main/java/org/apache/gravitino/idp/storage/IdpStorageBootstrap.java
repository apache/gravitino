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

package org.apache.gravitino.idp.storage;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.idp.auth.IdpServiceAdminManager;
import org.apache.gravitino.idp.auth.ServiceAdminInitializer;
import org.apache.gravitino.idp.storage.gc.IdpLegacyGarbageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One-time initialization for built-in IdP storage components that are not wired through core
 * entity-store lifecycle.
 *
 * <p>Not invoked from {@link
 * org.apache.gravitino.idp.storage.mapper.provider.IdpBasicMapperPackageProvider} in the current
 * PR; a future change can wire this through server/plugin lifecycle.
 */
public final class IdpStorageBootstrap {

  private static final Logger LOG = LoggerFactory.getLogger(IdpStorageBootstrap.class);

  private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);

  private IdpStorageBootstrap() {}

  /** Initializes IdP storage background tasks and service admins once per JVM. */
  public static void initializeOnce() {
    if (!INITIALIZED.compareAndSet(false, true)) {
      return;
    }

    Config config = GravitinoEnv.getInstance().config();
    try {
      ServiceAdminInitializer.getInstance()
          .initialize(config, IdpServiceAdminManager.fromEnvironment());
      IdpLegacyGarbageCollector.startScheduledCollector(config);
    } catch (RuntimeException e) {
      INITIALIZED.set(false);
      LOG.error("Failed to initialize built-in IdP storage", e);
      throw e;
    } catch (Exception e) {
      INITIALIZED.set(false);
      LOG.error("Failed to initialize built-in IdP storage", e);
      throw new IllegalStateException("Failed to initialize built-in IdP storage", e);
    }
  }
}
