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

import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

public class CreateGroup extends Command {
  protected final String metalake;
  protected final String group;

  /**
   * Create a new group.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param group The name of the group.
   */
  public CreateGroup(CommandContext context, String metalake, String group) {
    super(context);
    this.metalake = metalake;
    this.group = group;
  }

  /** Create a new group. */
  @Override
  public void handle() {
    try {
      GravitinoClient client = buildClient(metalake);
      client.addGroup(group);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (GroupAlreadyExistsException err) {
      exitWithError(ErrorMessages.GROUP_EXISTS);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(group + " created");
  }
}
