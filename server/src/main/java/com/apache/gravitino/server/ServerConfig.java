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
package com.apache.gravitino.server;

import com.apache.gravitino.Config;
import com.apache.gravitino.config.ConfigBuilder;
import com.apache.gravitino.config.ConfigConstants;
import com.apache.gravitino.config.ConfigEntry;

public class ServerConfig extends Config {

  public static final ConfigEntry<Integer> SERVER_SHUTDOWN_TIMEOUT =
      new ConfigBuilder("gravitino.server.shutdown.timeout")
          .doc("The stop idle timeout(millis) of the Gravitino Server")
          .version("0.1.0")
          .intConf()
          .checkValue(value -> value > 0, ConfigConstants.POSITIVE_NUMBER_ERROR_MSG)
          .createWithDefault(3 * 1000);

  public ServerConfig(boolean loadDefaults) {
    super(loadDefaults);
  }

  public ServerConfig() {
    this(true);
  }
}
