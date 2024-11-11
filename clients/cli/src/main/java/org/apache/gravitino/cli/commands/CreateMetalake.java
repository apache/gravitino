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

import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;

public class CreateMetalake extends Command {
  protected final String metalake;
  protected final String comment;

  /**
   * Create a new metalake.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param comment The metalake's comment.
   */
  public CreateMetalake(String url, boolean ignoreVersions, String metalake, String comment) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.comment = comment;
  }

  /** Create a new metalake. */
  @Override
  public void handle() {
    try {
      GravitinoAdminClient client = buildAdminClient();
      client.createMetalake(metalake, comment, null);
    } catch (MetalakeAlreadyExistsException err) {
      System.err.println(ErrorMessages.METALAKE_EXISTS);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    System.out.println(metalake + " created");
  }
}
