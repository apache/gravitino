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

package org.apache.gravitino.cli;

import com.google.common.base.Joiner;
import java.util.List;
import org.apache.commons.cli.CommandLine;

public abstract class CommandHandler {
  public static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();
  private String authEnv;
  private boolean authSet = false;

  /**
   * Retrieves the Gravitino authentication from the command line options or the GRAVITINO_AUTH
   * environment variable or the Gravitino config file.
   *
   * @param line The command line instance.
   * @return The Gravitino authentication, or null if not found.
   */
  public String getAuth(CommandLine line) {
    // If specified on the command line use that
    if (line.hasOption(GravitinoOptions.SIMPLE)) {
      return GravitinoOptions.SIMPLE;
    }

    // Cache the Gravitino authentication type environment variable
    if (authEnv == null && !authSet) {
      authEnv = System.getenv("GRAVITINO_AUTH");
      authSet = true;
    }

    // If set return the Gravitino authentication type environment variable
    if (authEnv != null) {
      return authEnv;
    }

    // Check if the authentication type is specified in the configuration file
    GravitinoConfig config = new GravitinoConfig(null);
    if (config.fileExists()) {
      config.read();
      String configAuthType = config.getGravitinoAuthType();
      if (configAuthType != null) {
        return configAuthType;
      }
    }

    return null;
  }

  protected abstract void handle();

  protected void checkEntities(List<String> entities) {
    if (!entities.isEmpty()) {
      System.err.println(ErrorMessages.MISSING_ENTITIES + COMMA_JOINER.join(entities));
      Main.exit(-1);
    }
  }
}
