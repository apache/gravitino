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

import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.GravitinoOptions;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/** Disable or enable metalake. */
public class ManageMetalake extends Command {
  private final String metalake;
  private final CommandLine line;
  private Boolean enableAllCatalogs;

  /**
   * Construct a new instance of the {@code ManageMetalake}.
   *
   * @param context the command context.
   * @param metalake the name of the metalake.
   */
  public ManageMetalake(CommandContext context, String metalake) {
    super(context);
    this.metalake = metalake;
    this.line = context.line();

    this.enableAllCatalogs = line.hasOption(GravitinoOptions.ALL);
  }

  /** Disable or enable the metalake. */
  @Override
  public void handle() {
    if (line.hasOption(GravitinoOptions.ENABLE)) {
      enableMetalake();
    } else if (line.hasOption(GravitinoOptions.DISABLE)) {
      disableMetalake();
    }
  }

  /** {@inheritDoc} */
  @Override
  public Command validate() {
    if (line.hasOption(GravitinoOptions.ENABLE) && line.hasOption(GravitinoOptions.DISABLE)) {
      exitWithError(ErrorMessages.INVALID_ENABLE_DISABLE);
    }

    return super.validate();
  }

  /** Enable the metalake. */
  private void enableMetalake() {
    StringBuilder msgBuilder = new StringBuilder(metalake);
    try {
      GravitinoAdminClient client = buildAdminClient();
      client.enableMetalake(metalake);
      msgBuilder.append(" has been enabled.");

      if (enableAllCatalogs) {
        GravitinoMetalake metalakeObject = client.loadMetalake(metalake);
        String[] catalogs = metalakeObject.listCatalogs();
        Arrays.stream(catalogs).forEach(metalakeObject::enableCatalog);
        msgBuilder.append(" and all catalogs in this metalake have been enabled.");
      }
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(msgBuilder.toString());
  }

  /** Disable the metalake. */
  private void disableMetalake() {
    try {
      GravitinoAdminClient client = buildAdminClient();
      client.disableMetalake(metalake);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(metalake + " has been disabled.");
  }
}
