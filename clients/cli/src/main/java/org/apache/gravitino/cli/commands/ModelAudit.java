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
import org.apache.gravitino.cli.ErrorMessages;
import org.apache.gravitino.client.GravitinoClient;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.model.Model;

/** Displays the audit information of a model. */
public class ModelAudit extends AuditCommand {

  protected final String metalake;
  protected final String catalog;
  protected final String model;

  /**
   * Displays the audit information of a model.
   *
   * @param url The URL of the Gravitino server.
   * @param ignoreVersions If true don't check the client/server versions match.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param model The name of the model.
   */
  public ModelAudit(
      String url, boolean ignoreVersions, String metalake, String catalog, String model) {
    super(url, ignoreVersions);
    this.metalake = metalake;
    this.catalog = catalog;
    this.model = model;
  }

  /** Displays the audit information of a model. */
  @Override
  public void handle() {
    Model result;

    try (GravitinoClient client = buildClient(this.metalake)) {
      NameIdentifier name = NameIdentifier.of(this.metalake, this.catalog, this.model);
      result = client.loadCatalog(this.catalog).asModelCatalog().getModel(name);
    } catch (NoSuchMetalakeException err) {
      System.err.println(ErrorMessages.UNKNOWN_METALAKE);
      return;
    } catch (NoSuchCatalogException err) {
      System.err.println(ErrorMessages.UNKNOWN_CATALOG);
      return;
    } catch (NoSuchModelException err) {
      System.err.println(ErrorMessages.UNKNOWN_MODEL);
      return;
    } catch (Exception exp) {
      System.err.println(exp.getMessage());
      return;
    }

    if (result != null) {
      displayAuditInfo(result.auditInfo());
    }
  }
}