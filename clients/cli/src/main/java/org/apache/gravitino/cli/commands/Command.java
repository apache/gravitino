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

import static org.apache.gravitino.client.GravitinoClientBase.Builder;

import org.apache.gravitino.cli.outputs.PlainFormat;
import org.apache.gravitino.cli.outputs.TableFormat;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/* The base for all commands. */
public abstract class Command {
  private final String url;
  private final boolean ignoreVersions;
  private final String outputFormat;
  public static String OUTPUT_FORMAT_TABLE = "table";
  public static String OUTPUT_FORMAT_PLAIN = "plain";

  protected static String authentication = null;
  protected static String userName = null;

  /**
   * Command constructor.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   */
  public Command(String url, boolean ignoreVersions) {
    this.url = url;
    this.ignoreVersions = ignoreVersions;
    this.outputFormat = OUTPUT_FORMAT_PLAIN;
  }

  /**
   * Command constructor.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param outputFormat output format used in some commands
   */
  public Command(String url, boolean ignoreVersions, String outputFormat) {
    this.url = url;
    this.ignoreVersions = ignoreVersions;
    this.outputFormat = outputFormat;
  }

  /**
   * Sets the authentication mode and user credentials for the command.
   *
   * @param authentication the authentication mode to be used (e.g. "simple")
   * @param userName the username associated with the authentication mode
   */
  public static void setAuthenicationMode(String authentication, String userName) {
    Command.authentication = authentication;
    Command.userName = userName;
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
    Builder<GravitinoClient> client = GravitinoClient.builder(url).withMetalake(metalake);

    if (ignoreVersions) {
      client = client.withVersionCheckDisabled();
    }
    if (authentication != null) {
      if (authentication.equals("simple")) {
        if (userName != null && !userName.isEmpty()) {
          client = client.withSimpleAuth(userName);
        } else {
          client = client.withSimpleAuth();
        }
      }
    }

    return client.build();
  }

  /**
   * Builds a {@link GravitinoAdminClient} instance with the server URL.
   *
   * @return A configured {@link GravitinoAdminClient} instance.
   */
  protected GravitinoAdminClient buildAdminClient() {
    Builder<GravitinoAdminClient> client = GravitinoAdminClient.builder(url);

    if (ignoreVersions) {
      client = client.withVersionCheckDisabled();
    }
    if (authentication != null) {
      if (authentication.equals("simple")) {
        if (userName != null && !userName.isEmpty()) {
          client = client.withSimpleAuth(userName);
        } else {
          client = client.withSimpleAuth();
        }
      }
    }

    return client.build();
  }

  /**
   * Outputs the entity to the console.
   *
   * @param entity The entity to output.
   */
  protected <T> void output(T entity) {
    if (outputFormat == null) {
      PlainFormat.output(entity);
      return;
    }

    if (outputFormat.equals(OUTPUT_FORMAT_TABLE)) {
      TableFormat.output(entity);
    } else if (outputFormat.equals(OUTPUT_FORMAT_PLAIN)) {
      PlainFormat.output(entity);
    } else {
      throw new IllegalArgumentException("Unsupported output format");
    }
  }
}
