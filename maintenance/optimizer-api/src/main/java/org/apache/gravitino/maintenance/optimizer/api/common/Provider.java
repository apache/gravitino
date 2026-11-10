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

package org.apache.gravitino.maintenance.optimizer.api.common;

import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;

/**
 * Base SPI for optimizer plug-ins (statistics, strategies, submitters). Implementations are
 * discovered through {@link java.util.ServiceLoader} and follow a simple lifecycle: instantiated
 * via default constructor, {@link #initialize(OptimizerEnv)} is called once before use, then {@link
 * #close()} is invoked when the optimizer shuts down.
 */
@DeveloperApi
public interface Provider extends AutoCloseable {

  /**
   * Stable provider name used for discovery, configuration lookup, and logging.
   *
   * @return provider name
   */
  String name();

  /**
   * Initialize the provider with the optimizer environment. This method is called once before the
   * provider is used. Implementations should use this method to wire external resources (clients,
   * credentials, caches) before first use.
   *
   * @param optimizerEnv shared optimizer environment/configuration
   */
  void initialize(OptimizerEnv optimizerEnv);
}
