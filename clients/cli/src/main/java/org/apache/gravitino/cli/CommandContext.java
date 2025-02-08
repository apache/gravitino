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

import org.apache.gravitino.cli.commands.Command;

/* Context for a command */
public class CommandContext {
  private String url;
  private boolean ignoreVersions;
  private boolean force;
  private String outputFormat;
  // Can add more "global" command flags here without any major changes e.g. a guiet flag

  /**
   * Command constructor.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   */
  public CommandContext(String url, boolean ignoreVersions) {
    this.url = url;
    this.ignoreVersions = ignoreVersions;
    this.force = false;
    this.outputFormat = Command.OUTPUT_FORMAT_PLAIN;
  }

  /**
   * Command constructor.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param force Force operation.
   * @param outputFormat Display output format.
   */
  public CommandContext(String url, boolean ignoreVersions, boolean force, String outputFormat) {
    this.url = url;
    this.ignoreVersions = ignoreVersions;
    this.force = force;
    this.outputFormat = outputFormat;
  }

  /**
   * Returns the URL.
   *
   * @return The URL.
   */
  public String url() {
    return url;
  }

  /**
   * Sets the URL.
   *
   * @param url The URL to be set.
   */
  public void setUrl(String url) {
    this.url = url;
  }

  /**
   * Indicates whether versions should be ignored.
   *
   * @return False if versions should be ignored.
   */
  public boolean ignoreVersions() {
    return ignoreVersions;
  }

  /**
   * Indicates whether the operation should be forced.
   *
   * @return True if the operation should be forced.
   */
  public boolean force() {
    return force;
  }

  /**
   * Returns the output format.
   *
   * @return The output format.
   */
  public String outputFormat() {
    return outputFormat;
  }
}
