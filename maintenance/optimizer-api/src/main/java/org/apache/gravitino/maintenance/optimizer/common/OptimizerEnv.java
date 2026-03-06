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

package org.apache.gravitino.maintenance.optimizer.common;

import java.util.Optional;
import org.apache.gravitino.maintenance.optimizer.common.conf.OptimizerConfig;

/**
 * Immutable container for runtime optimizer context passed into pluggable components. It carries
 * static configuration ({@link OptimizerConfig}) and optional command-scoped runtime content
 * ({@link OptimizerContent}).
 */
public class OptimizerEnv {
  // The config items from the config file
  private final OptimizerConfig config;
  private final OptimizerContent content;

  public OptimizerEnv(OptimizerConfig config) {
    this(config, null);
  }

  public OptimizerEnv(OptimizerConfig config, OptimizerContent content) {
    this.config = config;
    this.content = content;
  }

  public OptimizerConfig config() {
    return config;
  }

  public Optional<OptimizerContent> content() {
    return Optional.ofNullable(content);
  }

  public OptimizerEnv withContent(OptimizerContent content) {
    return new OptimizerEnv(config, content);
  }
}
