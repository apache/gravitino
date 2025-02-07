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
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;

/** Enable metalake. */
public class MetalakeEnable extends Command {

  private final String metalake;
  private Boolean enableAllCatalogs;

  /**
   * Enable a metalake
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param enableAllCatalogs Whether to enable all catalogs.
   */
  public MetalakeEnable(CommandContext context, String metalake, boolean enableAllCatalogs) {
    super(context);
    this.metalake = metalake;
    this.enableAllCatalogs = enableAllCatalogs;
  }

  /** Enable metalake. */
  @Override
  public void handle() {
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
}
