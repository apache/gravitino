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

package org.apache.gravitino.maintenance.optimizer.common.util;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

/** Utility methods for creating Gravitino clients from optimizer configuration. */
public final class GravitinoClientUtils {

  private GravitinoClientUtils() {}

  /**
   * Creates a {@link GravitinoClient} using optimizer configuration.
   *
   * <p>When the config carries auth keys (for example from update-stats {@code updater-options}
   * loaded into {@link OptimizerConfig}), those credentials are applied to the client builder.
   *
   * @param optimizerEnv optimizer environment
   * @return configured Gravitino client
   */
  public static GravitinoClient createClient(OptimizerEnv optimizerEnv) {
    return createClient(optimizerEnv, null);
  }

  /**
   * Creates a {@link GravitinoClient} using URI/metalake from {@code optimizerEnv} and optional
   * auth from {@code authFromUpdaterOptions} (short names such as {@code auth_type}).
   *
   * <p>Used by {@code submit-update-stats-job} so the CLI can call {@code runJob} against a secured
   * server with the same credentials that the job will use.
   *
   * @param optimizerEnv optimizer environment (URI and metalake)
   * @param authFromUpdaterOptions updater-options map, may be {@code null}
   * @return configured Gravitino client
   */
  public static GravitinoClient createClient(
      OptimizerEnv optimizerEnv, @Nullable Map<String, String> authFromUpdaterOptions) {
    Preconditions.checkArgument(optimizerEnv != null, "optimizerEnv must not be null");
    OptimizerConfig config = optimizerEnv.config();
    String uri = config.get(OptimizerConfig.GRAVITINO_URI_CONFIG);
    String metalake = config.get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG);
    GravitinoClient.ClientBuilder builder = GravitinoClient.builder(uri).withMetalake(metalake);

    OptimizerConfig authConfig = config;
    if (authFromUpdaterOptions != null && !authFromUpdaterOptions.isEmpty()) {
      Map<String, String> authProperties = new HashMap<>(authFromUpdaterOptions);
      GravitinoAuthSettings.copyAliases(authProperties);
      authConfig = new OptimizerConfig(authProperties);
    }
    GravitinoAuthSettings.from(authConfig).applyTo(builder);
    return builder.build();
  }
}
