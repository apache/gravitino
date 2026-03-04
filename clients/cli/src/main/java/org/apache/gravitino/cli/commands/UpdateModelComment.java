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

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.model.ModelChange;

/** Update the comment of a model. */
public class UpdateModelComment extends Command {
  /** The metalake name. */
  protected final String metalake;
  /** The catalog name. */
  protected final String catalog;
  /** The schema name. */
  protected final String schema;
  /** The model name. */
  protected final String model;
  /** The new comment. */
  protected final String comment;

  /**
   * Construct a new {@link UpdateModelComment} instance.
   *
   * @param context The command context.
   * @param metalake The metalake name.
   * @param catalog The catalog name.
   * @param schema The schema name.
   * @param model The model name.
   * @param comment The new comment.
   */
  public UpdateModelComment(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      String comment) {
    super(context);
    this.metalake = metalake;
    this.catalog = catalog;
    this.schema = schema;
    this.model = model;
    this.comment = comment;
  }

  /** Update the comment of a model. */
  @Override
  public void handle() {
    NameIdentifier modelIdent;

    try {
      modelIdent = NameIdentifier.of(schema, model);
      GravitinoClient client = buildClient(metalake);
      ModelChange updateCommentChange = ModelChange.updateComment(comment);

      client.loadCatalog(catalog).asModelCatalog().alterModel(modelIdent, updateCommentChange);
    } catch (NoSuchMetalakeException err) {
      exitWithError(ErrorMessages.UNKNOWN_METALAKE);
    } catch (NoSuchCatalogException err) {
      exitWithError(ErrorMessages.UNKNOWN_CATALOG);
    } catch (NoSuchSchemaException err) {
      exitWithError(ErrorMessages.UNKNOWN_SCHEMA);
    } catch (NoSuchModelException err) {
      exitWithError(ErrorMessages.UNKNOWN_MODEL);
    } catch (Exception exp) {
      exitWithError(exp.getMessage());
    }

    printInformation(model + " comment changed.");
  }
}
