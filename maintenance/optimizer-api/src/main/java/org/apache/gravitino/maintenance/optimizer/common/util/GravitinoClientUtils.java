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
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

/** Utility methods for creating Gravitino clients from optimizer configuration. */
public final class GravitinoClientUtils {

  private GravitinoClientUtils() {}

  /**
   * Creates a {@link GravitinoClient} using optimizer configuration.
   *
   * @param optimizerEnv optimizer environment
   * @return configured Gravitino client
   */
  public static GravitinoClient createClient(OptimizerEnv optimizerEnv) {
    Preconditions.checkArgument(optimizerEnv != null, "optimizerEnv must not be null");
    OptimizerConfig config = optimizerEnv.config();
    String uri = config.get(OptimizerConfig.GRAVITINO_URI_CONFIG);
    String metalake = config.get(OptimizerConfig.GRAVITINO_METALAKE_CONFIG);
    return GravitinoClient.builder(uri).withMetalake(metalake).build();
  }
}
