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

import org.apache.gravitino.Audit;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.cli.FullName;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;

/** List the names of all functions in a schema. */
public class ListFunctions extends Command {
  /** The name of the metalake. */
  protected final String metalake;
  /** The full name of the entity. */
  protected final FullName name;

  /**
   * Command constructor.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param fullName The {@link FullName} instance.
   */
  public ListFunctions(CommandContext context, String metalake, FullName fullName) {
    super(context);
    this.metalake = metalake;
    this.name = fullName;
  }

  /** {@inheritDoc} */
  @Override
  public void handle() {
    NameIdentifier[] functions = null;
    try {
      GravitinoClient gravitinoClient = buildClient(metalake);
      functions =
          gravitinoClient
              .loadCatalog(name.getCatalogName())
              .asFunctionCatalog()
              .listFunctions(Namespace.of(name.getSchemaName()));
    } catch (NoSuchMetalakeException ex) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException ex) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException ex) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (Exception ex) {
      exitWithError(ex.getMessage());
    }

    if (functions == null || functions.length == 0) {
      printInformation("No functions exist.");
      return;
    }

    Function[] gFunctions = new Function[functions.length];

    for (int i = 0; i < functions.length; i++) {
      String functionName = functions[i].name();
      gFunctions[i] =
          new Function() {
            @Override
            public String name() {
              return functionName;
            }

            @Override
            public FunctionType functionType() {
              return null;
            }

            @Override
            public boolean deterministic() {
              return false;
            }

            @Override
            public FunctionDefinition[] definitions() {
              return new FunctionDefinition[0];
            }

            @Override
            public Audit auditInfo() {
              return null;
            }
          };
    }

    printResults(gFunctions);
  }
}
