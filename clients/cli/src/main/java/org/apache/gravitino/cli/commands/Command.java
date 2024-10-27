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

package org.apache.gravitino.cli.commands;

import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/* The base for all commands. */
public abstract class Command {
  private final String url;
  private final boolean ignoreVersions;

  /**
   * Command constructor.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   */
  public Command(String url, boolean ignoreVersions) {
    this.url = url;
    this.ignoreVersions = ignoreVersions;
  }

  /** All commands have a handle method to handle and run the required command. */
  public abstract void handle();

  /**
   * Builds a {@link GravitinoClient} instance with the provided server URL and metalake.
   *
   * @param metalake The name of the metalake.
   * @return A configured {@link GravitinoClient} instance.
   * @throws NoSuchMetalakeException if the specified metalake does not exist.
   */
  protected GravitinoClient buildClient(String metalake) throws NoSuchMetalakeException {
    if (ignoreVersions) {
      return GravitinoClient.builder(url).withMetalake(metalake).withVersionCheckDisabled().build();
    } else {
      return GravitinoClient.builder(url).withMetalake(metalake).build();
    }
  }

  /**
   * Builds a {@link GravitinoAdminClient} instance with the server URL.
   *
   * @return A configured {@link GravitinoAdminClient} instance.
   */
  protected GravitinoAdminClient buildAdminClient() {
    if (ignoreVersions) {
      return GravitinoAdminClient.builder(url).withVersionCheckDisabled().build();
    } else {
      return GravitinoAdminClient.builder(url).build();
    }
  }
}
