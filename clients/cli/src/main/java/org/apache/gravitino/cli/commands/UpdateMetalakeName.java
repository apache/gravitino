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

import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.cli.AreYouSure;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/** Update the name of a metalake. */
public class UpdateMetalakeName extends Command {

  protected final String metalake;
  protected final String name;
  protected final boolean force;

  /**
   * Update the name of a metalake.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param name The new metalake name.
   */
  public UpdateMetalakeName(CommandContext context, String metalake, String name) {
    super(context);
    this.force = context.force();
    this.metalake = metalake;
    this.name = name;
  }

  /** Update the name of a metalake. */
  @Override
  public void handle() {

    if (!AreYouSure.really(force)) {
      return;
    }

    try {
      GravitinoAdminClient client = buildAdminClient();
      MetalakeChange change = MetalakeChange.rename(name);
      client.alterMetalake(metalake, change);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(metalake + " name changed.");
  }
}
